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
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.TableSpec;


/**
 * Table descriptor for RocksDb backed tables
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class RocksDbTableDescriptor<K, V> extends TableDescriptor<K, V, RocksDbTableDescriptor<K, V>> {

  static final public String KEY_SERDE = "key.serde";
  static final public String VALUE_SERDE = "value.serde";
  static final public String WRITE_BATCH_SIZE = "write.batch.size";
  static final public String OBJECT_CACHE_SIZE = "object.cache.size";
  static final public String CONTAINER_CACHE_SIZE_BYTES = "container.cache.size.bytes";
  static final public String CONTAINER_WRITE_BUFFER_SIZE_BYTES = "container.write.buffer.size.bytes";
  static final public String ROCKSDB_COMPRESSION = "rocksdb.compression";
  static final public String ROCKSDB_BLOCK_SIZE_BYTES = "rocksdb.block.size.bytes";
  static final public String ROCKSDB_TTL_MS = "rocksdb.ttl.ms";
  static final public String ROCKSDB_COMPACTION_STYLE = "rocksdb.compaction.style";
  static final public String ROCKSDB_NUM_WRITE_BUFFERS = "rocksdb.num.write.buffers";
  static final public String ROCKSDB_MAX_LOG_FILE_SIZE_BYTES = "rocksdb.max.log.file.size.bytes";
  static final public String ROCKSDB_KEEP_LOG_FILE_NUM = "rocksdb.keep.log.file.num";

  private Serde<K> keySerde;
  private Serde<V> valueSerde;
  private Integer writeBatchSize;
  private Integer objectCacheSize;
  private Integer cacheSize;
  private Integer writeBufferSize;
  private Integer blockSize;
  private Integer ttl;
  private Integer numOfWriteBuffers;
  private Integer maxLogFileSize;
  private Integer numOfLogFilesToKeep;
  private String compressionType;
  private String compactionStyle;

  public RocksDbTableDescriptor(String tableId) {
    super(tableId);
  }

  /**
   * Set the Serde for keys of this table
   * @param keySerde the key serde
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withKeySerde(Serde<K> keySerde) {
    this.keySerde = keySerde;
    return this;
  }

  /**
   * Set the Serde for values of this table
   * @param valueSerde the value serde
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withValueSerde(Serde<V> valueSerde) {
    this.valueSerde = valueSerde;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.write.batch.size</code> in Samza configuration guide
   * @param writeBatchSize write batch size
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withWriteBatchSize(int writeBatchSize) {
    this.writeBatchSize = writeBatchSize;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.object.cache.size</code> in Samza configuration guide
   * @param objectCacheSize the object cache size
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withObjectCacheSize(int objectCacheSize) {
    this.objectCacheSize = objectCacheSize;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.container.cache.size.bytes</code> in Samza configuration guide
   * @param cacheSize the cache size in bytes
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withCacheSize(int cacheSize) {
    this.cacheSize = cacheSize;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.container.write.buffer.size.bytes</code> in Samza configuration guide
   * @param writeBufferSize the write buffer size in bytes
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withWriteBufferSize(int writeBufferSize) {
    this.writeBufferSize = writeBufferSize;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.rocksdb.compression</code> in Samza configuration guide
   * @param compressionType the compression type
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withCompressionType(String compressionType) {
    this.compressionType = compressionType;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.rocksdb.block.size.bytes</code> in Samza configuration guide
   * @param blockSize the block size in bytes
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withBlockSize(int blockSize) {
    this.blockSize = blockSize;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.rocksdb.ttl.ms</code> in Samza configuration guide
   * @param ttl the time to live in milliseconds
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withTtl(int ttl) {
    this.ttl = ttl;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.rocksdb.compaction.style</code> in Samza configuration guide
   * @param compactionStyle the compaction style
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withCompactionStyle(String compactionStyle) {
    this.compactionStyle = compactionStyle;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.rocksdb.num.write.buffers</code> in Samza configuration guide
   * @param numOfWriteBuffers the number of write buffers
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withNumOfWriteBuffers(int numOfWriteBuffers) {
    this.numOfWriteBuffers = numOfWriteBuffers;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.rocksdb.max.log.file.size.bytes</code> in Samza configuration guide
   * @param maxLogFileSize the maximal log file size in bytes
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withMaxLogFileSize(int maxLogFileSize) {
    this.maxLogFileSize = maxLogFileSize;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.rocksdb.num.write.buffers</code> in Samza configuration guide
   * @param numOfLogFilesToKeep the number of log files to keep
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withNumOfLogFilesToKeep(int numOfLogFilesToKeep) {
    this.numOfLogFilesToKeep = numOfLogFilesToKeep;
    return this;
  }

  @Override
  public TableSpec getTableSpec() {

    validate();

    Map<String, String> map = new HashMap<>();

    map.putAll(config);

    map.put(KEY_SERDE, keySerde.getClass().getName());
    map.put(VALUE_SERDE, valueSerde.getClass().getName());

    if (writeBatchSize != null) {
      addRocksDbConfig(map, WRITE_BATCH_SIZE, writeBatchSize.toString());
    }
    if (objectCacheSize != null) {
      addRocksDbConfig(map, OBJECT_CACHE_SIZE, objectCacheSize.toString());
    }
    if (cacheSize != null) {
      addRocksDbConfig(map, CONTAINER_CACHE_SIZE_BYTES, cacheSize.toString());
    }
    if (writeBufferSize != null) {
      addRocksDbConfig(map, CONTAINER_WRITE_BUFFER_SIZE_BYTES, writeBufferSize.toString());
    }
    if (compressionType != null) {
      addRocksDbConfig(map, ROCKSDB_COMPRESSION, compressionType);
    }
    if (blockSize != null) {
      addRocksDbConfig(map, ROCKSDB_BLOCK_SIZE_BYTES, blockSize.toString());
    }
    if (ttl != null) {
      addRocksDbConfig(map, ROCKSDB_TTL_MS, ttl.toString());
    }
    if (compactionStyle != null) {
      addRocksDbConfig(map, ROCKSDB_COMPACTION_STYLE, compactionStyle);
    }
    if (numOfWriteBuffers != null) {
      addRocksDbConfig(map, ROCKSDB_NUM_WRITE_BUFFERS, numOfWriteBuffers.toString());
    }
    if (maxLogFileSize != null) {
      addRocksDbConfig(map, ROCKSDB_MAX_LOG_FILE_SIZE_BYTES, maxLogFileSize.toString());
    }
    if (numOfLogFilesToKeep != null) {
      addRocksDbConfig(map, ROCKSDB_KEEP_LOG_FILE_NUM, numOfLogFilesToKeep.toString());
    }

    return new TableSpec(tableId, RocksDbTableProviderFactory.class.getName(), map);
  }

  private void addRocksDbConfig(Map<String, String> map, String key, String value) {
    map.put("rocksdb." + key, value);
  }

  @Override
  protected void validate() {
    if (keySerde == null) {
      throw new SamzaException("Key serde not configured");
    }

    if (valueSerde == null) {
      throw new SamzaException("Value serde not configured");
    }
  }

  static public class Factory<K, V> implements TableDescriptor.Factory<RocksDbTableDescriptor> {
    @Override
    public RocksDbTableDescriptor<K, V> getTableDescriptor(String tableId) {
      return new RocksDbTableDescriptor(tableId);
    }
  }
}
