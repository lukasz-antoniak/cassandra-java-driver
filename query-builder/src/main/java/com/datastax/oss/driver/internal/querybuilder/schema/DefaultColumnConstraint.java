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
package com.datastax.oss.driver.internal.querybuilder.schema;

import com.datastax.oss.driver.api.querybuilder.schema.ColumnConstraint;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import edu.umd.cs.findbugs.annotations.NonNull;

public class DefaultColumnConstraint implements ColumnConstraint {
  private final String functionName;
  private final String operator;
  private final Term rightOperand;
  private String columnName;

  public DefaultColumnConstraint(String functionName, String operator, Term rightOperand) {
    this.functionName = functionName;
    this.operator = operator;
    this.rightOperand = rightOperand;
  }

  @Override
  public DefaultColumnConstraint withColumnName(String columnName) {
    this.columnName = columnName;
    return this;
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    if (functionName != null) {
      builder.append(functionName).append("(").append(columnName).append(")");
    } else {
      builder.append(columnName);
    }
    builder.append(" ").append(operator).append(" ");
    rightOperand.appendTo(builder);
  }
}
