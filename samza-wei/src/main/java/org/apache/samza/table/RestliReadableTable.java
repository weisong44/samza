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
package org.apache.samza.table;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;


public class RestliReadableTable<K, V> implements ReadableTable<K, V> {

  private MyRestClient restClient;
  private RestliTableFunction<K, V> tableFn;

  public RestliReadableTable(MyRestClient restClient, RestliTableFunction<K, V> tableFn) {
    this.restClient = restClient;
    this.tableFn = tableFn;
  }

  @Override
  public V get(K key) {
    return tableFn.get(key, restClient);
  }

  @Override
  public Map<K, V> getAll(List<K> keys) {
    try {
      return tableFn.getAll(keys, restClient);
    } catch (NotImplementedException ex) {
      return keys.stream()
          .map(key -> new ImmutablePair<>(key, tableFn.get(key, restClient)))
          .collect(Collectors.toMap(p -> p.left, p -> p.right));
    }
  }

  @Override
  public void close() {
  }
}
