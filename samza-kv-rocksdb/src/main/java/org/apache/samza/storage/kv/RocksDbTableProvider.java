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
package org.apache.samza.storage.kv;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.config.JavaSerializerConfig;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.storage.StorageEngine;
import org.apache.samza.table.StoreBackedTableProvider;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableSpec;


/**
 * TODO-wsong
 */
public class RocksDbTableProvider implements StoreBackedTableProvider {

  private final TableSpec tableSpec;

  private KeyValueStore kvStore;

  public RocksDbTableProvider(TableSpec tableSpec) {
    this.tableSpec = tableSpec;
  }

  @Override
  public Map<String, String> generateConfig() {

    Map<String, String> configs = new HashMap<>();

    // Store configuration
    configs.put(String.format(
        "stores.%s.factory", tableSpec.getId()),
        RocksDbKeyValueStorageEngineFactory.class.getName());
    configs.put(String.format(
        "stores.%s.key.serde", tableSpec.getId()),
        getOrConfigureSerdeName(configs, tableSpec.getConfig().get(RocksDbTableDescriptor.KEY_SERDE)));
    configs.put(String.format(
        "stores.%s.msg.serde", tableSpec.getId()),
        getOrConfigureSerdeName(configs, tableSpec.getConfig().get(RocksDbTableDescriptor.VALUE_SERDE)));

    // Rest of the configuration
    tableSpec.getConfig().forEach((k, v) -> {
      String realKey = k.startsWith("rocksdb.") ?
          String.format("stores.%s", tableSpec.getId()) + "." + k.substring("rocksdb.".length())
        : String.format(JavaTableConfig.TABLE_ID_PREFIX, tableSpec.getId()) + "." + k;
      configs.put(realKey, v);
    });

    return configs;
  }

  @Override
  public void init(StorageEngine store) {
    kvStore = (KeyValueStore) store;
  }

  private String getOrConfigureSerdeName(Map<String, String> configs, String serdeClass) {
    JavaSerializerConfig serdeConfigs = new JavaSerializerConfig(new MapConfig(configs));
    for (String serdeName : serdeConfigs.getSerdeNames()) {
      String configuredSerdeClass = serdeConfigs.getSerdeClass(serdeName);
      if (configuredSerdeClass.equals(serdeClass)) {
        return serdeName;
      }
    }

    // Add the serde to the config
    String serdeName = serdeClass.replace(".", "_");
    configs.put(String.format("serializers.registry.%s.class", serdeName), serdeClass + "Factory");
    return serdeName;
  }

  @Override
  public Table getTable() {
    if (kvStore == null) {
      throw new SamzaException("Store not initialized for table " + tableSpec.getId());
    }
    return new StoreBackedReadWriteTable(kvStore);
  }

  @Override
  public void start() {
  }

  @Override
  public void shutdown() {
  }

}
