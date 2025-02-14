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

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.schema.ColumnConstraint;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import net.jcip.annotations.Immutable;

/** Class encapsulating all attributes related to column definition. */
@Immutable
public class ColumnDefinition {
  private final DataType type;
  private final ImmutableList<ColumnConstraint> constraints;

  public ColumnDefinition(DataType type) {
    this(type, ImmutableList.of());
  }

  public ColumnDefinition(DataType type, ImmutableList<ColumnConstraint> constraints) {
    this.type = type;
    this.constraints = constraints;
  }

  public DataType getType() {
    return type;
  }

  public ImmutableList<ColumnConstraint> getConstraints() {
    return constraints;
  }
}
