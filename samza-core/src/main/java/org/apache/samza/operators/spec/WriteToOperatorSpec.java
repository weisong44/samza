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

import java.util.function.Function;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.table.TableSpec;


/**
 * TODO: wsong
 * @param <K>
 * @param <V>
 * @param <M>
 */
@InterfaceStability.Unstable
public class WriteToOperatorSpec<K, V, M> extends OperatorSpec<M, Void> {

  private TableSpec tableSpec;
  private Function<? super M, ? extends K> keyExtractor;
  private Function<? super M, ? extends V> valueExtractor;

  /**
   * TODO: wsong
   */
  WriteToOperatorSpec(TableSpec tableSpec, Function<? super M, ? extends K> keyExtractor,
      Function<? super M, ? extends V> valueExtractor, int opId) {
    super(OpCode.WRITE_TO, opId);
    this.tableSpec = tableSpec;
    this.keyExtractor = keyExtractor;
    this.valueExtractor = valueExtractor;
  }

  public TableSpec getTableSpec() {
    return tableSpec;
  }

  public Function<? super M, ? extends K> getKeyExtractor() {
    return keyExtractor;
  }

  public Function<? super M, ? extends V> getValueExtractor() {
    return valueExtractor;
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return null;
  }
}
