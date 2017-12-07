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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.config.JavaTableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RestliTableProvider implements TableProvider {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final TableSpec tableSpec;
  private final RestliTableFunction tableFn;

  private MyRestClient restClient;
  private RestliReadableTable table;

  public RestliTableProvider(TableSpec tableSpec) {
    this.tableSpec = tableSpec;
    String serializedTableFn = tableSpec.getConfig().get(RestliTableDescriptor.TABLE_FN);
    byte[] bytes = Base64.getDecoder().decode(serializedTableFn);
    tableFn = fromBytes(bytes);
  }

  @Override
  public Table getTable() {
    return table;
  }

  @Override
  public Map<String, String> generateConfig(Map<String, String> config) {

    Map<String, String> tableConfig = new HashMap<>();

    tableConfig.put(
        getTableFnKey(),
        tableSpec.getConfig().get(RestliTableDescriptor.TABLE_FN));

    logger.info("Generated configuration for table " + tableSpec.getId());

    return tableConfig;
  }

  @Override
  public void start() {
    restClient = new MyRestClient();
    table = new RestliReadableTable(restClient, tableFn);
  }

  @Override
  public void stop() {
    if (restClient != null) {
      restClient.close();
      restClient = null;
      table = null;
    }
  }

  public RestliTableFunction fromBytes(byte[] bytes) {
    if (bytes != null) {
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      ObjectInputStream ois = null;

      try {
        ois = new ObjectInputStream(bis);
        return (RestliTableFunction) ois.readObject();
      } catch (IOException | ClassNotFoundException e) {
        throw new SamzaException("Error reading from input stream.");
      } finally {
        try {
          if (ois != null) {
            ois.close();
          }
        } catch (IOException e) {
          throw new SamzaException("Error closing input stream", e);
        }
      }
    } else {
      return null;
    }
  }

  private String getTableFnKey() {
    return String.format(JavaTableConfig.TABLE_ID_PREFIX, tableSpec.getId()) + "." + RestliTableDescriptor.TABLE_FN;
  }
}
