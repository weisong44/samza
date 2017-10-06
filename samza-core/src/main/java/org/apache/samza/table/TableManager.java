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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.storage.StorageEngine;
import org.apache.samza.util.Util;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * TODO: wsong
 */
public class TableManager {

  static public class TableCtx {
    private TableSpec tableSpec;
    private TableProvider tableProvider;
  }

  // tableId -> TableCtx
  private Map<String, TableCtx> tables = new HashMap<>();

  public TableManager(Config config) {
    ObjectMapper objectMapper = new ObjectMapper();
    new JavaTableConfig(config).getTableIds().forEach(tableId -> {
        String tableSpecJson = config.get(String.format(JavaTableConfig.TABLE_SPEC, tableId));
        TableSpec tableSpec = null;
        if (tableSpecJson != null) {
          try {
            tableSpec = objectMapper.readValue(tableSpecJson, TableSpec.class);
          } catch (IOException ex) {
            // If this step fails, we fill the table spec using config
          }
        }

        if (tableSpec == null) {
          String tableProviderFactory = config.get(String.format(JavaTableConfig.TABLE_PROVIDER_FACTORY, tableId));
          tableSpec = new TableSpec(tableId, tableProviderFactory,
              config.subset(String.format(JavaTableConfig.TABLE_ID_PREFIX, tableId) + "."));
        }

        addTable(tableSpec);
      });
  }

  public TableManager(Config config, Map<String, StorageEngine> stores) {

    this(config);

    tables.values().forEach(ctx -> {
        if (ctx.tableProvider instanceof StoreBackedTableProvider) {
          StorageEngine store = stores.get(ctx.tableSpec.getId());
          if (store == null) {
            throw new SamzaException(String.format(
                "Backing store for table %s was not injected by SamzaContainer",
                ctx.tableSpec.getId()));
          }
          ((StoreBackedTableProvider) ctx.tableProvider).init(store);
        }
      });
  }

  public void addTable(TableDescriptor tableDescriptor) {
    addTable(tableDescriptor.getTableSpec());
  }

  public void addTable(TableSpec tableSpec) {
    if (tables.containsKey(tableSpec.getId())) {
      throw new SamzaException("Table " + tableSpec.getId() + " already exists");
    }
    TableCtx ctx = new TableCtx();
    TableProviderFactory tableProviderFactory = Util.getObj(tableSpec.getTableProviderFactory());
    ctx.tableProvider = tableProviderFactory.getTableProvider(tableSpec);
    ctx.tableSpec = tableSpec;
    tables.put(tableSpec.getId(), ctx);
  }

  public void start() {
    tables.values().forEach(ctx -> ctx.tableProvider.start());
  }

  public void shutdown() {
    tables.values().forEach(ctx -> ctx.tableProvider.start());
  }

  public Table getTable(String tableId) {
    return tables.get(tableId).tableProvider.getTable();
  }

  public Table getTable(TableSpec tableSpec) {
    return getTable(tableSpec.getId());
  }
}
