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

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.config.MapConfig;
import org.apache.samza.storage.StorageEngine;
import org.junit.Test;

import junit.framework.Assert;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestTableManager {

  public static final String TABLE_SPEC_JSON =
      "{\"id\":\"t1\",\"tableProviderFactory\":\"" + DummyTableProviderFactory.class.getName() + "\","
    + "\"config\":{\"value.serde\":\"org.apache.samza.serializers.IntegerSerde\","
    + "\"key.serde\":\"org.apache.samza.serializers.IntegerSerde\"}}";

  public static class DummyTableProviderFactory implements TableProviderFactory {

    static Table table;
    static StoreBackedTableProvider tableProvider;

    @Override
    public TableProvider getTableProvider(TableSpec tableSpec) {
      table = mock(Table.class);
      tableProvider = mock(StoreBackedTableProvider.class);
      when(tableProvider.getTable()).thenReturn(table);
      return tableProvider;
    }
  }

  @Test
  public void testInitByJson() {
    Map<String, String> map = new HashMap<>();
    map.put("tables.t1.spec", TABLE_SPEC_JSON);
    doTestInit(map);
  }

  @Test
  public void testInitByConfig() {
    Map<String, String> map = new HashMap<>();
    map.put("tables.t1.provider.factory", DummyTableProviderFactory.class.getName());
    doTestInit(map);
  }

  private void doTestInit(Map<String, String> map) {
    Map<String, StorageEngine> storageEngines = new HashMap<>();
    storageEngines.put("t1", mock(StorageEngine.class));

    TableManager tableManager = new TableManager(new MapConfig(map), storageEngines);

    Table table = tableManager.getTable("t1");
    verify(DummyTableProviderFactory.tableProvider, times(1)).init(anyObject());
    Assert.assertEquals(DummyTableProviderFactory.table, table);
  }
}
