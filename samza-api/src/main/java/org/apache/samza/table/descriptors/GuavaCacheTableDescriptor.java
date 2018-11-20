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

package org.apache.samza.table.descriptors;

import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.table.utils.SerdeUtils;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;

/**
 * Table descriptor for Guava-based caching table.
 * @param <K> type of the key in the cache
 * @param <V> type of the value in the cache
 */
public class GuavaCacheTableDescriptor<K, V> extends BaseTableDescriptor<K, V, GuavaCacheTableDescriptor<K, V>> {

  public static final String PROVIDER_FACTORY_CLASS_NAME = "org.apache.samza.table.caching.guava.GuavaCacheTableProviderFactory";

  public static final String GUAVA_CACHE = "guavaCache";

  private Cache<K, V> cache;

  /**
   * {@inheritDoc}
   */
  public GuavaCacheTableDescriptor(String tableId) {
    super(tableId);
  }

  /**
   * Specify a pre-configured Guava cache instance to be used for caching table.
   * @param cache Guava cache instance
   * @return this descriptor
   */
  public GuavaCacheTableDescriptor withCache(Cache<K, V> cache) {
    this.cache = cache;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getProviderFactoryClassName() {
    return PROVIDER_FACTORY_CLASS_NAME;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void generateConfig(Config jobConfig, Map<String, String> tableConfig) {
    super.generateConfig(jobConfig, tableConfig);
    addTableConfig(GUAVA_CACHE, SerdeUtils.serialize("Guava cache", cache), tableConfig);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void validate() {
    super.validate();
    Preconditions.checkArgument(cache != null, "Must provide a Guava cache instance.");
  }
}
