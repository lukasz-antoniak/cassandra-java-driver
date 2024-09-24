/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NodeUnavailableException;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.BootstrappingException;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.FunctionFailureException;
import com.datastax.oss.driver.api.core.servererrors.ProtocolError;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.adminrequest.ThrottledAdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.UnexpectedResponseException;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.ResponseCallback;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RepreparePayload;
import com.datastax.oss.driver.internal.core.tracker.NoopRequestTracker;
import com.datastax.oss.driver.internal.core.tracker.RequestLogger;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import com.datastax.oss.protocol.internal.response.result.SchemaChange;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.response.result.Void;
import com.datastax.oss.protocol.internal.util.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.handler.codec.EncoderException;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CqlRequestHandler implements Throttled {

  private static final Logger LOG = LoggerFactory.getLogger(CqlRequestHandler.class);
  private static final long NANOTIME_NOT_MEASURED_YET = -1;

  private final long startTimeNanos;
  private long endTimeNanos = NANOTIME_NOT_MEASURED_YET;
  private final String logPrefix;
  private final Statement<?> initialStatement;
  private final DefaultSession session;
  private final CqlIdentifier keyspace;
  private final InternalDriverContext context;
  protected final CompletableFuture<AsyncResultSet> result;
  private final Timer timer;
  /**
   * How many speculative executions are currently running (including the initial execution). We
   * track this in order to know when to fail the request if all executions have reached the end of
   * the query plan.
   */
  private final AtomicInteger activeExecutionsCount;
  /**
   * How many speculative executions have started (excluding the initial execution), whether they
   * have completed or not. We track this in order to fill {@link
   * ExecutionInfo#getSpeculativeExecutionCount()}.
   */
  private final AtomicInteger startedSpeculativeExecutionsCount;

  final Timeout scheduledTimeout;
  final List<Timeout> scheduledExecutions;
  private final List<NodeResponseCallback> inFlightCallbacks;
  private final RequestThrottler throttler;
  private final RequestTracker requestTracker;
  private final SessionMetricUpdater sessionMetricUpdater;
  private final DriverExecutionProfile executionProfile;

  // The errors on the nodes that were already tried (lazily initialized on the first error).
  // We don't use a map because nodes can appear multiple times.
  private volatile List<Map.Entry<Node, Throwable>> errors;

  protected CqlRequestHandler(
      Statement<?> statement,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {

    this.startTimeNanos = System.nanoTime();
    this.logPrefix = sessionLogPrefix + "|" + this.hashCode();
    LOG.trace("[{}] Creating new handler for request {}", logPrefix, statement);

    this.initialStatement = statement;
    this.session = session;
    this.keyspace = session.getKeyspace().orElse(null);
    this.context = context;
    this.result = new CompletableFuture<>();
    this.result.exceptionally(
        t -> {
          try {
            if (t instanceof CancellationException) {
              cancelScheduledTasks();
            }
          } catch (Throwable t2) {
            Loggers.warnWithException(LOG, "[{}] Uncaught exception", logPrefix, t2);
          }
          return null;
        });

    this.activeExecutionsCount = new AtomicInteger(1);
    this.startedSpeculativeExecutionsCount = new AtomicInteger(0);
    this.scheduledExecutions = new CopyOnWriteArrayList<>();
    this.inFlightCallbacks = new CopyOnWriteArrayList<>();

    this.requestTracker = context.getRequestTracker();
    this.sessionMetricUpdater = session.getMetricUpdater();

    this.timer = context.getNettyOptions().getTimer();
    this.executionProfile = Conversions.resolveExecutionProfile(initialStatement, context);
    Duration timeout = Conversions.resolveRequestTimeout(statement, executionProfile);
    this.scheduledTimeout = scheduleTimeout(timeout);

    trackStart();
    this.throttler = context.getRequestThrottler();
    this.throttler.register(this);
  }

  @Override
  public void onThrottleReady(boolean wasDelayed) {
    if (wasDelayed
        // avoid call to nanoTime() if metric is disabled:
        && sessionMetricUpdater.isEnabled(
            DefaultSessionMetric.THROTTLING_DELAY, executionProfile.getName())) {
      sessionMetricUpdater.updateTimer(
          DefaultSessionMetric.THROTTLING_DELAY,
          executionProfile.getName(),
          System.nanoTime() - startTimeNanos,
          TimeUnit.NANOSECONDS);
    }
    Queue<Node> queryPlan =
        this.initialStatement.getNode() != null
            ? new SimpleQueryPlan(this.initialStatement.getNode())
            : context
                .getLoadBalancingPolicyWrapper()
                .newQueryPlan(initialStatement, executionProfile.getName(), session);
    sendRequest(initialStatement, null, queryPlan, 0, 0, true);
  }

  public CompletionStage<AsyncResultSet> handle() {
    return result;
  }

  private Timeout scheduleTimeout(Duration timeoutDuration) {
    if (timeoutDuration.toNanos() > 0) {
      try {
        return this.timer.newTimeout(
            (Timeout timeout1) -> {
              ExecutionInfo executionInfo = failedExecutionInfoNoRequestSent().build();
              setFinalError(
                  executionInfo,
                  new DriverTimeoutException("Query timed out after " + timeoutDuration));
            },
            timeoutDuration.toNanos(),
            TimeUnit.NANOSECONDS);
      } catch (IllegalStateException e) {
        // If we raced with session shutdown the timer might be closed already, rethrow with a more
        // explicit message
        result.completeExceptionally(
            "cannot be started once stopped".equals(e.getMessage())
                ? new IllegalStateException("Session is closed")
                : e);
      }
    }
    return null;
  }

  /**
   * Sends the request to the next available node.
   *
   * @param statement The statement to execute.
   * @param retriedNode if not null, it will be attempted first before the rest of the query plan.
   * @param queryPlan the list of nodes to try (shared with all other executions)
   * @param currentExecutionIndex 0 for the initial execution, 1 for the first speculative one, etc.
   * @param retryCount the number of times that the retry policy was invoked for this execution
   *     already (note that some internal retries don't go through the policy, and therefore don't
   *     increment this counter)
   * @param scheduleNextExecution whether to schedule the next speculative execution
   */
  private void sendRequest(
      Statement<?> statement,
      Node retriedNode,
      Queue<Node> queryPlan,
      int currentExecutionIndex,
      int retryCount,
      boolean scheduleNextExecution) {
    if (result.isDone()) {
      return;
    }
    Node node = retriedNode;
    DriverChannel channel = null;
    if (node == null || (channel = session.getChannel(node, logPrefix)) == null) {
      while (!result.isDone() && (node = queryPlan.poll()) != null) {
        channel = session.getChannel(node, logPrefix);
        if (channel != null) {
          break;
        } else {
          recordError(node, new NodeUnavailableException(node));
        }
      }
    }
    if (channel == null) {
      // We've reached the end of the query plan without finding any node to write to
      if (!result.isDone() && activeExecutionsCount.decrementAndGet() == 0) {
        // We're the last execution so fail the result
        ExecutionInfo executionInfo = failedExecutionInfoNoRequestSent(statement).build();
        setFinalError(executionInfo, AllNodesFailedException.fromErrors(this.errors));
      }
    } else {
      NodeResponseCallback nodeResponseCallback =
          new NodeResponseCallback(
              statement,
              node,
              queryPlan,
              channel,
              currentExecutionIndex,
              retryCount,
              scheduleNextExecution,
              logPrefix);
      Message message = Conversions.toMessage(statement, executionProfile, context);
      trackNodeStart(statement, node, nodeResponseCallback.logPrefix);
      channel
          .write(message, statement.isTracing(), statement.getCustomPayload(), nodeResponseCallback)
          .addListener(nodeResponseCallback);
    }
  }

  private void recordError(Node node, Throwable error) {
    // Use a local variable to do only a single volatile read in the nominal case
    List<Map.Entry<Node, Throwable>> errorsSnapshot = this.errors;
    if (errorsSnapshot == null) {
      synchronized (CqlRequestHandler.this) {
        errorsSnapshot = this.errors;
        if (errorsSnapshot == null) {
          this.errors = errorsSnapshot = new CopyOnWriteArrayList<>();
        }
      }
    }
    errorsSnapshot.add(new AbstractMap.SimpleEntry<>(node, error));
  }

  private void cancelScheduledTasks() {
    if (this.scheduledTimeout != null) {
      this.scheduledTimeout.cancel();
    }
    if (scheduledExecutions != null) {
      for (Timeout scheduledExecution : scheduledExecutions) {
        scheduledExecution.cancel();
      }
    }
    for (NodeResponseCallback callback : inFlightCallbacks) {
      callback.cancel();
    }
  }

  private void setFinalResult(
      Result resultMessage,
      Frame responseFrame,
      boolean schemaInAgreement,
      NodeResponseCallback callback) {
    try {
      ExecutionInfo executionInfo =
          defaultExecutionInfo(callback)
              .withServerResponse(resultMessage, responseFrame)
              .withSchemaInAgreement(schemaInAgreement)
              .build();
      AsyncResultSet resultSet =
          Conversions.toResultSet(resultMessage, executionInfo, session, context);
      if (result.complete(resultSet)) {
        cancelScheduledTasks();
        throttler.signalSuccess(this);

        long endTimeNanos = trackNodeEnd(callback, executionInfo, null);
        trackEnd(executionInfo, null);
        if (sessionMetricUpdater.isEnabled(
            DefaultSessionMetric.CQL_REQUESTS, executionProfile.getName())) {
          // Only call nanoTime() if we're actually going to use it
          if (endTimeNanos == NANOTIME_NOT_MEASURED_YET) {
            endTimeNanos = System.nanoTime();
          }
          long totalLatencyNanos = endTimeNanos - startTimeNanos;
          sessionMetricUpdater.updateTimer(
              DefaultSessionMetric.CQL_REQUESTS,
              executionProfile.getName(),
              totalLatencyNanos,
              TimeUnit.NANOSECONDS);
        }
      }
      // log the warnings if they have NOT been disabled
      if (!executionInfo.getWarnings().isEmpty()
          && executionProfile.getBoolean(DefaultDriverOption.REQUEST_LOG_WARNINGS)
          && LOG.isWarnEnabled()) {
        logServerWarnings(callback.statement, executionProfile, executionInfo.getWarnings());
      }
    } catch (Throwable error) {
      // something unpredictable unexpected happened here that we can't blame on the request itself
      ExecutionInfo executionInfo = defaultExecutionInfo(callback, -1).build();
      setFinalError(executionInfo, error);
    }
  }

  private void logServerWarnings(
      Statement<?> statement, DriverExecutionProfile executionProfile, List<String> warnings) {
    // use the RequestLogFormatter to format the query
    StringBuilder statementString = new StringBuilder();
    context
        .getRequestLogFormatter()
        .appendRequest(
            statement,
            executionProfile.getInt(
                DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH,
                RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_QUERY_LENGTH),
            executionProfile.getBoolean(
                DefaultDriverOption.REQUEST_LOGGER_VALUES,
                RequestLogger.DEFAULT_REQUEST_LOGGER_SHOW_VALUES),
            executionProfile.getInt(
                DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES,
                RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_VALUES),
            executionProfile.getInt(
                DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH,
                RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_VALUE_LENGTH),
            statementString);
    // log each warning separately
    warnings.forEach(
        (warning) ->
            LOG.warn("Query '{}' generated server side warning(s): {}", statementString, warning));
  }

  @Override
  public void onThrottleFailure(@NonNull RequestThrottlingException error) {
    sessionMetricUpdater.incrementCounter(
        DefaultSessionMetric.THROTTLING_ERRORS, executionProfile.getName());
    ExecutionInfo executionInfo = failedExecutionInfoNoRequestSent().build();
    setFinalError(executionInfo, error);
  }

  private void setFinalError(ExecutionInfo executionInfo, Throwable error) {
    if (error instanceof DriverException) {
      ((DriverException) error).setExecutionInfo(executionInfo);
    }
    if (result.completeExceptionally(error)) {
      cancelScheduledTasks();
      trackEnd(executionInfo, error);
      if (error instanceof DriverTimeoutException) {
        throttler.signalTimeout(this);
        sessionMetricUpdater.incrementCounter(
            DefaultSessionMetric.CQL_CLIENT_TIMEOUTS,
            executionInfo.getExecutionProfile().getName());
      } else if (!(error instanceof RequestThrottlingException)) {
        throttler.signalError(this, error);
      }
    }
  }

  /**
   * Handles the interaction with a single node in the query plan.
   *
   * <p>An instance of this class is created each time we (re)try a node.
   */
  private class NodeResponseCallback
      implements ResponseCallback, GenericFutureListener<Future<java.lang.Void>> {

    private final long nodeStartTimeNanos = System.nanoTime();
    private long nodeEndTimeNanos = NANOTIME_NOT_MEASURED_YET;
    private final Statement<?> statement;
    private final Node node;
    private final Queue<Node> queryPlan;
    private final DriverChannel channel;
    // The identifier of the current execution (0 for the initial execution, 1 for the first
    // speculative execution, etc.)
    private final int execution;
    // How many times we've invoked the retry policy and it has returned a "retry" decision (0 for
    // the first attempt of each execution).
    private final int retryCount;
    private final boolean scheduleNextExecution;
    private final String logPrefix;

    private NodeResponseCallback(
        Statement<?> statement,
        Node node,
        Queue<Node> queryPlan,
        DriverChannel channel,
        int execution,
        int retryCount,
        boolean scheduleNextExecution,
        String logPrefix) {
      this.statement = statement;
      this.node = node;
      this.queryPlan = queryPlan;
      this.channel = channel;
      this.execution = execution;
      this.retryCount = retryCount;
      this.scheduleNextExecution = scheduleNextExecution;
      this.logPrefix = logPrefix + "|" + execution;
    }

    // this gets invoked once the write request completes.
    @Override
    public void operationComplete(Future<java.lang.Void> future) throws Exception {
      if (!future.isSuccess()) {
        Throwable error = future.cause();
        ExecutionInfo executionInfo = CqlRequestHandler.this.defaultExecutionInfo(this).build();
        if (error instanceof EncoderException
            && error.getCause() instanceof FrameTooLongException) {
          trackNodeEnd(this, executionInfo, error.getCause());
          setFinalError(executionInfo, error.getCause());
        } else {
          LOG.trace(
              "[{}] Failed to send request on {}, trying next node (cause: {})",
              logPrefix,
              channel,
              error.getMessage(),
              error);
          recordError(node, error);
          trackNodeEnd(this, executionInfo, error);
          ((DefaultNode) node)
              .getMetricUpdater()
              .incrementCounter(DefaultNodeMetric.UNSENT_REQUESTS, executionProfile.getName());
          sendRequest(
              statement,
              null,
              queryPlan,
              execution,
              retryCount,
              scheduleNextExecution); // try next node
        }
      } else {
        LOG.trace("[{}] Request sent on {}", logPrefix, channel);
        if (result.isDone()) {
          // If the handler completed since the last time we checked, cancel directly because we
          // don't know if cancelScheduledTasks() has run yet
          cancel();
        } else {
          inFlightCallbacks.add(this);
          if (scheduleNextExecution
              && Conversions.resolveIdempotence(statement, executionProfile)) {
            int nextExecution = execution + 1;
            long nextDelay;
            try {
              nextDelay =
                  Conversions.resolveSpeculativeExecutionPolicy(context, executionProfile)
                      .nextExecution(node, keyspace, statement, nextExecution);
            } catch (Throwable cause) {
              // This is a bug in the policy, but not fatal since we have at least one other
              // execution already running. Don't fail the whole request.
              LOG.error(
                  "[{}] Unexpected error while invoking the speculative execution policy",
                  logPrefix,
                  cause);
              return;
            }
            if (nextDelay >= 0) {
              scheduleSpeculativeExecution(nextExecution, nextDelay);
            } else {
              LOG.trace(
                  "[{}] Speculative execution policy returned {}, no next execution",
                  logPrefix,
                  nextDelay);
            }
          }
        }
      }
    }

    private void scheduleSpeculativeExecution(int index, long delay) {
      LOG.trace("[{}] Scheduling speculative execution {} in {} ms", logPrefix, index, delay);
      try {
        scheduledExecutions.add(
            timer.newTimeout(
                (Timeout timeout1) -> {
                  if (!result.isDone()) {
                    LOG.trace(
                        "[{}] Starting speculative execution {}",
                        CqlRequestHandler.this.logPrefix,
                        index);
                    activeExecutionsCount.incrementAndGet();
                    startedSpeculativeExecutionsCount.incrementAndGet();
                    // Note that `node` is the first node of the execution, it might not be the
                    // "slow" one if there were retries, but in practice retries are rare.
                    ((DefaultNode) node)
                        .getMetricUpdater()
                        .incrementCounter(
                            DefaultNodeMetric.SPECULATIVE_EXECUTIONS, executionProfile.getName());
                    sendRequest(statement, null, queryPlan, index, 0, true);
                  }
                },
                delay,
                TimeUnit.MILLISECONDS));
      } catch (IllegalStateException e) {
        // If we're racing with session shutdown, the timer might be stopped already. We don't want
        // to schedule more executions anyway, so swallow the error.
        if (!"cannot be started once stopped".equals(e.getMessage())) {
          Loggers.warnWithException(
              LOG, "[{}] Error while scheduling speculative execution", logPrefix, e);
        }
      }
    }

    @Override
    public void onResponse(Frame responseFrame) {
      long nodeResponseTimeNanos = NANOTIME_NOT_MEASURED_YET;
      NodeMetricUpdater nodeMetricUpdater = ((DefaultNode) node).getMetricUpdater();
      if (nodeMetricUpdater.isEnabled(DefaultNodeMetric.CQL_MESSAGES, executionProfile.getName())) {
        nodeResponseTimeNanos = System.nanoTime();
        long nodeLatency = nodeResponseTimeNanos - nodeStartTimeNanos;
        nodeMetricUpdater.updateTimer(
            DefaultNodeMetric.CQL_MESSAGES,
            executionProfile.getName(),
            nodeLatency,
            TimeUnit.NANOSECONDS);
      }
      inFlightCallbacks.remove(this);
      if (result.isDone()) {
        return;
      }
      try {
        Message responseMessage = responseFrame.message;
        if (responseMessage instanceof SchemaChange) {
          SchemaChange schemaChange = (SchemaChange) responseMessage;
          context
              .getMetadataManager()
              .refreshSchema(schemaChange.keyspace, false, false)
              .whenComplete(
                  (result, error) -> {
                    boolean schemaInAgreement;
                    if (error != null) {
                      Loggers.warnWithException(
                          LOG,
                          "[{}] Unexpected error while refreshing schema after DDL query, "
                              + "keeping previous version",
                          logPrefix,
                          error);
                      schemaInAgreement = false;
                    } else {
                      schemaInAgreement = result.isSchemaInAgreement();
                    }
                    setFinalResult(schemaChange, responseFrame, schemaInAgreement, this);
                  });
        } else if (responseMessage instanceof SetKeyspace) {
          SetKeyspace setKeyspace = (SetKeyspace) responseMessage;
          session
              .setKeyspace(CqlIdentifier.fromInternal(setKeyspace.keyspace))
              .whenComplete((v, error) -> setFinalResult(setKeyspace, responseFrame, true, this));
        } else if (responseMessage instanceof Result) {
          LOG.trace("[{}] Got result, completing", logPrefix);
          setFinalResult((Result) responseMessage, responseFrame, true, this);
        } else if (responseMessage instanceof Error) {
          LOG.trace("[{}] Got error response, processing", logPrefix);
          processErrorResponse((Error) responseMessage, responseFrame);
        } else {
          ExecutionInfo executionInfo = defaultExecutionInfo().build();
          IllegalStateException error =
              new IllegalStateException("Unexpected response " + responseMessage);
          trackNodeEnd(this, executionInfo, error);
          setFinalError(executionInfo, error);
        }
      } catch (Throwable t) {
        ExecutionInfo executionInfo = defaultExecutionInfo().build();
        trackNodeEnd(this, executionInfo, t);
        setFinalError(executionInfo, t);
      }
    }

    private void processErrorResponse(Error errorMessage, Frame errorFrame) {
      if (errorMessage.code == ProtocolConstants.ErrorCode.UNPREPARED) {
        ByteBuffer idToReprepare = ByteBuffer.wrap(((Unprepared) errorMessage).id);
        LOG.trace(
            "[{}] Statement {} is not prepared on {}, repreparing",
            logPrefix,
            Bytes.toHexString(idToReprepare),
            node);
        RepreparePayload repreparePayload = session.getRepreparePayloads().get(idToReprepare);
        if (repreparePayload == null) {
          throw new IllegalStateException(
              String.format(
                  "Tried to execute unprepared query %s but we don't have the data to reprepare it",
                  Bytes.toHexString(idToReprepare)));
        }
        Prepare reprepareMessage = repreparePayload.toMessage();
        ThrottledAdminRequestHandler<ByteBuffer> reprepareHandler =
            ThrottledAdminRequestHandler.prepare(
                channel,
                true,
                reprepareMessage,
                repreparePayload.customPayload,
                Conversions.resolveRequestTimeout(statement, executionProfile),
                throttler,
                sessionMetricUpdater,
                logPrefix);
        PrepareRequest reprepareRequest = Conversions.toPrepareRequest(reprepareMessage);
        long reprepareStartNanos = System.nanoTime();
        trackNodeEnd(
            this,
            defaultExecutionInfo().withServerResponse(errorFrame).build(),
            new IllegalStateException("Unexpected response " + errorMessage));
        // TODO: Shall we have different logPrefix?
        trackReprepareStatementStart(reprepareRequest, this, logPrefix);
        reprepareHandler
            .start()
            .handle(
                (repreparedId, exception) -> {
                  ExecutionInfo executionInfo = defaultExecutionInfo().build();
                  if (exception != null) {
                    // If the error is not recoverable, surface it to the client instead of retrying
                    if (exception instanceof UnexpectedResponseException) {
                      Message prepareErrorMessage =
                          ((UnexpectedResponseException) exception).message;
                      if (prepareErrorMessage instanceof Error) {
                        CoordinatorException prepareError =
                            Conversions.toThrowable(node, (Error) prepareErrorMessage, context);
                        if (prepareError instanceof QueryValidationException
                            || prepareError instanceof FunctionFailureException
                            || prepareError instanceof ProtocolError) {
                          LOG.trace("[{}] Unrecoverable error on reprepare, rethrowing", logPrefix);
                          trackReprepareStatementEnd(
                              reprepareRequest, this, prepareError, reprepareStartNanos, logPrefix);
                          trackNodeEnd(this, executionInfo, prepareError);
                          setFinalError(executionInfo, prepareError);
                          return null;
                        }
                      }
                    } else if (exception instanceof RequestThrottlingException) {
                      trackReprepareStatementEnd(
                          reprepareRequest, this, exception, reprepareStartNanos, logPrefix);
                      trackNodeEnd(this, executionInfo, exception);
                      setFinalError(executionInfo, exception);
                      return null;
                    }
                    recordError(node, exception);
                    trackReprepareStatementEnd(
                        reprepareRequest, this, exception, reprepareStartNanos, logPrefix);
                    trackNodeEnd(this, executionInfo, exception);
                    setFinalError(executionInfo, exception);
                    LOG.trace("[{}] Reprepare failed, trying next node", logPrefix);
                    sendRequest(statement, null, queryPlan, execution, retryCount, false);
                  } else {
                    if (!repreparedId.equals(idToReprepare)) {
                      IllegalStateException illegalStateException =
                          new IllegalStateException(
                              String.format(
                                  "ID mismatch while trying to reprepare (expected %s, got %s). "
                                      + "This prepared statement won't work anymore. "
                                      + "This usually happens when you run a 'USE...' query after "
                                      + "the statement was prepared.",
                                  Bytes.toHexString(idToReprepare),
                                  Bytes.toHexString(repreparedId)));
                      // notify error in initial statement execution
                      trackNodeEnd(this, executionInfo, illegalStateException);
                      setFinalError(executionInfo, illegalStateException);
                    }
                    LOG.trace("[{}] Reprepare successful, retrying", logPrefix);
                    // notify statement preparation as successful
                    trackReprepareStatementEnd(
                        reprepareRequest, this, null, reprepareStartNanos, logPrefix);
                    // do not report to onRequestStart(), because we already did during first
                    // attempt
                    sendRequest(statement, node, queryPlan, execution, retryCount, false);
                  }
                  return null;
                });
        return;
      }
      CoordinatorException error = Conversions.toThrowable(node, errorMessage, context);
      NodeMetricUpdater metricUpdater = ((DefaultNode) node).getMetricUpdater();
      if (error instanceof BootstrappingException) {
        LOG.trace("[{}] {} is bootstrapping, trying next node", logPrefix, node);
        ExecutionInfo executionInfo = defaultExecutionInfo().build();
        recordError(node, error);
        trackNodeEnd(this, executionInfo, error);
        sendRequest(statement, null, queryPlan, execution, retryCount, false);
      } else if (error instanceof QueryValidationException
          || error instanceof FunctionFailureException
          || error instanceof ProtocolError) {
        LOG.trace("[{}] Unrecoverable error, rethrowing", logPrefix);
        metricUpdater.incrementCounter(DefaultNodeMetric.OTHER_ERRORS, executionProfile.getName());
        ExecutionInfo executionInfo = defaultExecutionInfo().build();
        trackNodeEnd(this, executionInfo, error);
        setFinalError(executionInfo, error);
      } else {
        RetryPolicy retryPolicy = Conversions.resolveRetryPolicy(context, executionProfile);
        RetryVerdict verdict;
        if (error instanceof ReadTimeoutException) {
          ReadTimeoutException readTimeout = (ReadTimeoutException) error;
          verdict =
              retryPolicy.onReadTimeoutVerdict(
                  statement,
                  readTimeout.getConsistencyLevel(),
                  readTimeout.getBlockFor(),
                  readTimeout.getReceived(),
                  readTimeout.wasDataPresent(),
                  retryCount);
          updateErrorMetrics(
              metricUpdater,
              verdict,
              DefaultNodeMetric.READ_TIMEOUTS,
              DefaultNodeMetric.RETRIES_ON_READ_TIMEOUT,
              DefaultNodeMetric.IGNORES_ON_READ_TIMEOUT);
        } else if (error instanceof WriteTimeoutException) {
          WriteTimeoutException writeTimeout = (WriteTimeoutException) error;
          verdict =
              Conversions.resolveIdempotence(statement, executionProfile)
                  ? retryPolicy.onWriteTimeoutVerdict(
                      statement,
                      writeTimeout.getConsistencyLevel(),
                      writeTimeout.getWriteType(),
                      writeTimeout.getBlockFor(),
                      writeTimeout.getReceived(),
                      retryCount)
                  : RetryVerdict.RETHROW;
          updateErrorMetrics(
              metricUpdater,
              verdict,
              DefaultNodeMetric.WRITE_TIMEOUTS,
              DefaultNodeMetric.RETRIES_ON_WRITE_TIMEOUT,
              DefaultNodeMetric.IGNORES_ON_WRITE_TIMEOUT);
        } else if (error instanceof UnavailableException) {
          UnavailableException unavailable = (UnavailableException) error;
          verdict =
              retryPolicy.onUnavailableVerdict(
                  statement,
                  unavailable.getConsistencyLevel(),
                  unavailable.getRequired(),
                  unavailable.getAlive(),
                  retryCount);
          updateErrorMetrics(
              metricUpdater,
              verdict,
              DefaultNodeMetric.UNAVAILABLES,
              DefaultNodeMetric.RETRIES_ON_UNAVAILABLE,
              DefaultNodeMetric.IGNORES_ON_UNAVAILABLE);
        } else {
          verdict =
              Conversions.resolveIdempotence(statement, executionProfile)
                  ? retryPolicy.onErrorResponseVerdict(statement, error, retryCount)
                  : RetryVerdict.RETHROW;
          updateErrorMetrics(
              metricUpdater,
              verdict,
              DefaultNodeMetric.OTHER_ERRORS,
              DefaultNodeMetric.RETRIES_ON_OTHER_ERROR,
              DefaultNodeMetric.IGNORES_ON_OTHER_ERROR);
        }
        processRetryVerdict(verdict, error);
      }
    }

    private void processRetryVerdict(RetryVerdict verdict, Throwable error) {
      LOG.trace("[{}] Processing retry decision {}", logPrefix, verdict);
      ExecutionInfo executionInfo = defaultExecutionInfo().build();
      switch (verdict.getRetryDecision()) {
        case RETRY_SAME:
          recordError(node, error);
          trackNodeEnd(this, executionInfo, error);
          sendRequest(
              verdict.getRetryRequest(statement),
              node,
              queryPlan,
              execution,
              retryCount + 1,
              false);
          break;
        case RETRY_NEXT:
          recordError(node, error);
          trackNodeEnd(this, executionInfo, error);
          sendRequest(
              verdict.getRetryRequest(statement),
              null,
              queryPlan,
              execution,
              retryCount + 1,
              false);
          break;
        case RETHROW:
          trackNodeEnd(this, executionInfo, error);
          setFinalError(executionInfo, error);
          break;
        case IGNORE:
          setFinalResult(Void.INSTANCE, null, true, this);
          break;
      }
    }

    private void updateErrorMetrics(
        NodeMetricUpdater metricUpdater,
        RetryVerdict verdict,
        DefaultNodeMetric error,
        DefaultNodeMetric retriesOnError,
        DefaultNodeMetric ignoresOnError) {
      metricUpdater.incrementCounter(error, executionProfile.getName());
      switch (verdict.getRetryDecision()) {
        case RETRY_SAME:
        case RETRY_NEXT:
          metricUpdater.incrementCounter(DefaultNodeMetric.RETRIES, executionProfile.getName());
          metricUpdater.incrementCounter(retriesOnError, executionProfile.getName());
          break;
        case IGNORE:
          metricUpdater.incrementCounter(DefaultNodeMetric.IGNORES, executionProfile.getName());
          metricUpdater.incrementCounter(ignoresOnError, executionProfile.getName());
          break;
        case RETHROW:
          // nothing to do
      }
    }

    @Override
    public void onFailure(Throwable error) {
      inFlightCallbacks.remove(this);
      if (result.isDone()) {
        return;
      }
      LOG.trace("[{}] Request failure, processing: {}", logPrefix, error.getMessage(), error);
      RetryVerdict verdict;
      if (!Conversions.resolveIdempotence(statement, executionProfile)
          || error instanceof FrameTooLongException) {
        verdict = RetryVerdict.RETHROW;
      } else {
        try {
          RetryPolicy retryPolicy = Conversions.resolveRetryPolicy(context, executionProfile);
          verdict = retryPolicy.onRequestAbortedVerdict(statement, error, retryCount);
        } catch (Throwable cause) {
          ExecutionInfo executionInfo = defaultExecutionInfo().build();
          setFinalError(
              executionInfo,
              new IllegalStateException("Unexpected error while invoking the retry policy", cause));
          return;
        }
      }
      processRetryVerdict(verdict, error);
      updateErrorMetrics(
          ((DefaultNode) node).getMetricUpdater(),
          verdict,
          DefaultNodeMetric.ABORTED_REQUESTS,
          DefaultNodeMetric.RETRIES_ON_ABORTED,
          DefaultNodeMetric.IGNORES_ON_ABORTED);
    }

    public void cancel() {
      try {
        if (!channel.closeFuture().isDone()) {
          this.channel.cancel(this);
        }
      } catch (Throwable t) {
        Loggers.warnWithException(LOG, "[{}] Error cancelling", logPrefix, t);
      }
    }

    private DefaultExecutionInfo.Builder defaultExecutionInfo() {
      return CqlRequestHandler.this.defaultExecutionInfo(this, execution);
    }

    @Override
    public String toString() {
      return logPrefix;
    }
  }

  private DefaultExecutionInfo.Builder defaultExecutionInfo(NodeResponseCallback callback) {
    return defaultExecutionInfo(callback, callback.execution);
  }

  private DefaultExecutionInfo.Builder defaultExecutionInfo(
      NodeResponseCallback callback, int execution) {
    return new DefaultExecutionInfo.Builder(
        callback.statement,
        callback.node,
        startedSpeculativeExecutionsCount.get(),
        execution,
        errors,
        session,
        context,
        executionProfile);
  }

  private DefaultExecutionInfo.Builder failedExecutionInfoNoRequestSent() {
    return failedExecutionInfoNoRequestSent(initialStatement);
  }

  private DefaultExecutionInfo.Builder failedExecutionInfoNoRequestSent(Statement<?> statement) {
    return new DefaultExecutionInfo.Builder(
        statement,
        null,
        startedSpeculativeExecutionsCount.get(),
        -1,
        errors,
        session,
        context,
        executionProfile);
  }

  /** Notify request tracker that processing of initial statement starts. */
  private void trackStart() {
    trackStart(initialStatement, logPrefix);
  }

  /** Notify request tracker that processing of given statement starts. */
  private void trackStart(Request request, String logPrefix) {
    if (requestTracker instanceof NoopRequestTracker) {
      return;
    }
    requestTracker.onRequestCreated(request, executionProfile, logPrefix);
  }

  /** Notify request tracker that processing of given statement starts at a certain node. */
  private void trackNodeStart(Request request, Node node, String logPrefix) {
    if (requestTracker instanceof NoopRequestTracker) {
      return;
    }
    requestTracker.onRequestCreatedForNode(request, executionProfile, node, logPrefix);
  }

  /** Utility method to trigger {@link RequestTracker} based on {@link NodeResponseCallback}. */
  private long trackNodeEnd(
      NodeResponseCallback callback, ExecutionInfo executionInfo, Throwable error) {
    callback.nodeEndTimeNanos =
        trackNodeEndInternal(
            executionInfo.getRequest(),
            executionInfo.getCoordinator(),
            executionInfo,
            error,
            callback.nodeStartTimeNanos,
            callback.nodeEndTimeNanos,
            callback.logPrefix);
    return callback.nodeEndTimeNanos;
  }

  /**
   * Notify request tracker that processing of initial statement has been completed (successfully or
   * with error).
   */
  private void trackEnd(ExecutionInfo executionInfo, Throwable error) {
    endTimeNanos =
        trackEndInternal(
            initialStatement,
            executionInfo.getCoordinator(),
            executionInfo,
            error,
            startTimeNanos,
            endTimeNanos,
            logPrefix);
  }

  /**
   * Notify request tracker that processing of statement has been completed by a given node. To
   * minimalize number of calls to {@code System#nanoTime()} caller may pass end timestamp. If
   * passed timestamp equals {@code NANOTIME_NOT_MEASURED_YET}, method returns current end timestamp
   * for further reuse.
   */
  private long trackNodeEndInternal(
      Request request,
      Node node,
      ExecutionInfo executionInfo,
      Throwable error,
      long startTimeNanos,
      long endTimeNanos,
      String logPrefix) {
    if (requestTracker instanceof NoopRequestTracker) {
      return NANOTIME_NOT_MEASURED_YET;
    }
    endTimeNanos = endTimeNanos == -1 ? System.nanoTime() : endTimeNanos;
    long latencyNanos = endTimeNanos - startTimeNanos;
    if (error == null) {
      requestTracker.onNodeSuccess(
          request, latencyNanos, executionProfile, node, executionInfo, logPrefix);
    } else {
      requestTracker.onNodeError(
          request, error, latencyNanos, executionProfile, node, executionInfo, logPrefix);
    }
    return endTimeNanos;
  }

  /**
   * Notify request tracker that processing of given statement has been completed (successfully or
   * with error).
   */
  private long trackEndInternal(
      Request request,
      Node node,
      ExecutionInfo executionInfo,
      Throwable error,
      long startTimeNanos,
      long endTimeNanos,
      String logPrefix) {
    if (requestTracker instanceof NoopRequestTracker) {
      return NANOTIME_NOT_MEASURED_YET;
    }
    endTimeNanos = endTimeNanos == NANOTIME_NOT_MEASURED_YET ? System.nanoTime() : endTimeNanos;
    long latencyNanos = endTimeNanos - startTimeNanos;
    if (error == null) {
      requestTracker.onSuccess(
          request, latencyNanos, executionProfile, node, executionInfo, logPrefix);
    } else {
      requestTracker.onError(
          request, error, latencyNanos, executionProfile, node, executionInfo, logPrefix);
    }
    return endTimeNanos;
  }

  /**
   * Utility method to notify request tracker about start execution of re-prepating prepared
   * statement.
   */
  private void trackReprepareStatementStart(
      Request reprepareRequest, NodeResponseCallback callback, String logPrefix) {
    trackStart(reprepareRequest, logPrefix);
    trackNodeStart(reprepareRequest, callback.node, logPrefix);
  }

  /**
   * Utility method to notify request tracker about completed execution of re-prepating prepared
   * statement.
   */
  private void trackReprepareStatementEnd(
      Request statement,
      NodeResponseCallback callback,
      Throwable error,
      long startTimeNanos,
      String logPrefix) {
    ExecutionInfo executionInfo = defaultReprepareExecutionInfo(statement, callback.node).build();
    long endTimeNanos =
        trackNodeEndInternal(
            statement,
            callback.node,
            executionInfo,
            error,
            startTimeNanos,
            NANOTIME_NOT_MEASURED_YET,
            logPrefix);
    trackEndInternal(
        statement, callback.node, executionInfo, error, startTimeNanos, endTimeNanos, logPrefix);
  }

  private DefaultExecutionInfo.Builder defaultReprepareExecutionInfo(Request statement, Node node) {
    return new DefaultExecutionInfo.Builder(
        statement, node, -1, 0, null, session, context, executionProfile);
  }
}
