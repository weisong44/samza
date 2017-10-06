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

package org.apache.samza.example;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;


public class ArraySystemConsumer implements SystemConsumer {
  private final Config config;

  public ArraySystemConsumer(Config config) {
    this.config = config;
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }

  @Override
  public void register(SystemStreamPartition systemStreamPartition, String s) {
  }

  @Override
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Set<SystemStreamPartition> set, long l) throws InterruptedException {
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> envelopeMap = new HashMap<>();
    set.forEach(ssp -> {
        List<IncomingMessageEnvelope> envelopes = Arrays.stream(getArrayObjects(ssp.getSystemStream().getStream(), config))
          .map(object -> new IncomingMessageEnvelope(ssp, null, null, object))
          .collect(Collectors.toList());
        envelopes.add(IncomingMessageEnvelope.buildEndOfStreamEnvelope(ssp));
        envelopeMap.put(ssp, envelopes);
      });
    return envelopeMap;
  }

  private static Object[] getArrayObjects(String stream, Config config) {
    try {
      return Base64Serializer.deserialize(config.get("streams." + stream + ".source"), Object[].class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
