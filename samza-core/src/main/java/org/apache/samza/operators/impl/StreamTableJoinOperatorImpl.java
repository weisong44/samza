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
package org.apache.samza.operators.impl;

import java.util.Collection;
import java.util.Collections;

import org.apache.samza.config.Config;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.StreamTableJoinOperatorSpec;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;


/**
 * TODO: wsong
 * @param <K>
 * @param <M>
 * @param <R>
 * @param <OM>
 */
class StreamTableJoinOperatorImpl<K, M, R, OM> extends OperatorImpl<M, OM> {

  private final StreamTableJoinOperatorSpec<K, M, R, OM> joinOpSpec;
  private final ReadableTable<K, R> table;

  StreamTableJoinOperatorImpl(StreamTableJoinOperatorSpec<K, M, R, OM> joinOpSpec,
      Config config, TaskContext context) {
    this.joinOpSpec = joinOpSpec;
    this.table = (ReadableTable) context.getTable(joinOpSpec.getTableSpec().getId());
  }

  @Override
  protected void handleInit(Config config, TaskContext context) {
    this.joinOpSpec.getJoinFn().init(config, context);
  }

  @Override
  public Collection<OM> handleMessage(M message, MessageCollector collector, TaskCoordinator coordinator) {
    R record = table.get(joinOpSpec.getJoinFn().getFirstKey(message));
    return record != null ?
        Collections.singletonList(joinOpSpec.getJoinFn().apply(message, record))
      : Collections.emptyList();
  }

  @Override
  protected void handleClose() {
    this.joinOpSpec.getJoinFn().close();
  }

  protected OperatorSpec<M, OM> getOperatorSpec() {
    return (OperatorSpec<M, OM>) joinOpSpec;
  }

}
