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

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A helper class for handling table configuration
 */
public class JavaTableConfig extends MapConfig {

  // Prefix
  public static final String TABLES_PREFIX = "tables.";
  public static final String TABLE_ID_PREFIX = TABLES_PREFIX + "%s";

  // Suffix
  public static final String TABLE_PROVIDER_FACTORY_SUFFIX = ".provider.factory";

  // Config keys
  public static final String TABLE_PROVIDER_FACTORY = String.format("%s.provider.factory", TABLE_ID_PREFIX);
  public static final String TABLE_SPEC = String.format("%s.spec", TABLE_ID_PREFIX);
  public static final String TABLE_KEY_SERDE = String.format("%s.key.serde", TABLE_ID_PREFIX);
  public static final String TABLE_VALUE_SERDE = String.format("%s.value.serde", TABLE_ID_PREFIX);


  public JavaTableConfig(Config config) {
    super(config);
  }

  /**
   * Get Id's of all tables
   * @return list of table Id's
   */
  public List<String> getTableIds() {
    Config subConfig = subset(TABLES_PREFIX, true);
    Set<String> tableNames = subConfig.keySet().stream()
        .filter(k -> k.endsWith(TABLE_PROVIDER_FACTORY_SUFFIX))
        .map(k -> k.substring(0, k.indexOf(".")))
        .collect(Collectors.toSet());
    return new LinkedList<>(tableNames);
  }

  /**
   * Get the {@link org.apache.samza.table.TableProviderFactory} class for a table
   * @param tableId Id of the table
   * @return the {@link org.apache.samza.table.TableProviderFactory} class
   */
  public String getTableProviderFactory(String tableId) {
    return get(String.format(TABLE_PROVIDER_FACTORY, tableId), null);
  }

  /**
   * Get the {@link org.apache.samza.table.TableSpec} Json
   * @param tableId Id of the table
   * @return {@link org.apache.samza.table.TableSpec} as Json
   */
  public String getTableSpecJson(String tableId) {
    return get(String.format(TABLE_SPEC, tableId), null);
  }

}
