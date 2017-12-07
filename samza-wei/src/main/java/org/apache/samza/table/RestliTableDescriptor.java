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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.operators.BaseTableDescriptor;


public class RestliTableDescriptor<K, V>
    extends BaseTableDescriptor<K, V, RestliTableDescriptor<K, V>> {

  static public final String TABLE_FN = "table.fn";

  private RestliTableFunction<K, V> tableFn;

  public RestliTableDescriptor(String tableId) {
    super(tableId);
  }

  public RestliTableDescriptor withFn(RestliTableFunction<K, V> tableFn) {
    this.tableFn = tableFn;
    return this;
  }

  @Override
  protected void generateTableSpecConfig(Map<String, String> tableSpecConfig) {

    super.generateTableSpecConfig(tableSpecConfig);

    byte[] bytes = tableFnToBytes();
    tableSpecConfig.put(TABLE_FN, Base64.getEncoder().encodeToString(bytes));

  }

  @Override
  public void validate() {
    super.validate();
    if (tableFn == null) {
      throw new SamzaException("Table function tableFn not provided");
    }
  }

  @Override
  public TableSpec getTableSpec() {

    validate();

    Map<String, String> tableSpecConfig = new HashMap<>();
    generateTableSpecConfig(tableSpecConfig);

    return new TableSpec(tableId, serde, RestliTableProviderFactory.class.getName(), tableSpecConfig);
  }

  public byte[] tableFnToBytes() {
    if (tableFn != null) {
      ObjectOutputStream oos = null;
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try {
        oos = new ObjectOutputStream(bos);
        oos.writeObject(tableFn);
      } catch (IOException ex) {
        throw new SamzaException("Error writing to output stream", ex);
      } finally {
        try {
          if (oos != null) {
            oos.close();
          }
        } catch (IOException ex) {
          throw new SamzaException("Error closing output stream", ex);
        }
      }

      return bos.toByteArray();
    } else {
      return null;
    }
  }
}
