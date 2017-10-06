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
package org.apache.samza.config;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


public class JavaTableConfig extends MapConfig {

  public static final String TABLES_PREFIX = "tables.";
  public static final String TABLE_ID_PREFIX = TABLES_PREFIX + "%s";
  public static final String TABLE_PROVIDER_FACTORY = String.format("%s.provider.factory", TABLE_ID_PREFIX);
  public static final String TABLE_SPEC = String.format("%s.spec", TABLE_ID_PREFIX);

  public static final String TABLE_SPEC_SUFFIX = ".spec";
  public static final String TABLE_PROVIDER_FACTORY_SUFFIX = ".provider.factory";

  public JavaTableConfig(Config config) {
    super(config);
  }

  public List<String> getTableIds() {
    Config subConfig = subset(TABLES_PREFIX, true);
    Set<String> tableNames = new HashSet<>();
    for (String key : subConfig.keySet()) {
      if (key.endsWith(TABLE_SPEC_SUFFIX) || key.endsWith(TABLE_PROVIDER_FACTORY_SUFFIX)) {
        tableNames.add(key.substring(0, key.indexOf(".")));
      }
    }
    return new LinkedList<>(tableNames);
  }

  public String getTableSpecJson(String tableId) {
    return get(String.format(TABLE_SPEC, tableId), null);
  }

  public String getTableProviderFactory(String tableId) {
    return get(String.format(TABLE_PROVIDER_FACTORY, tableId), null);
  }
}
