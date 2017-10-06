/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.operators.spec;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.table.TableSpec;


/**
 * TODO: wsong
 * @param <K>
 * @param <M>
 * @param <R>
 * @param <OM>
 */
@InterfaceStability.Unstable
public class StreamTableJoinOperatorSpec<K, M, R, OM> extends OperatorSpec<Object, OM> { // Object == M | R

  private final OperatorSpec<?, M> leftInputOpSpec;
  private final TableSpec tableSpec;
  private final StreamTableJoinFunction<K, M, R, OM> joinFn;

  StreamTableJoinOperatorSpec(OperatorSpec<?, M> leftInputOpSpec, TableSpec tableSpec,
      StreamTableJoinFunction<K, M, R, OM> joinFn, int opId) {
    super(OpCode.JOIN, opId);
    this.leftInputOpSpec = leftInputOpSpec;
    this.tableSpec = tableSpec;
    this.joinFn = joinFn;
  }

  public OperatorSpec getLeftInputOpSpec() {
    return leftInputOpSpec;
  }

  public TableSpec getTableSpec() {
    return tableSpec;
  }

  public StreamTableJoinFunction<K, M, R, OM> getJoinFn() {
    return this.joinFn;
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return joinFn instanceof WatermarkFunction ? (WatermarkFunction) joinFn : null;
  }
}
