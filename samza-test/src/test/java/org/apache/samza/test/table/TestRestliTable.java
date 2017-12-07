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
package org.apache.samza.test.table;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.standalone.PassthroughCoordinationUtilsFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.table.RestliTableDescriptor;
import org.apache.samza.table.Table;
import org.apache.samza.test.harness.AbstractIntegrationTestHarness;
import org.apache.samza.test.table.TestTableData.EnrichedPageView;
import org.apache.samza.test.table.TestTableData.PageView;
import org.apache.samza.test.table.TestTableData.Profile;
import org.apache.samza.test.util.ArraySystemFactory;
import org.apache.samza.test.util.Base64Serializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestRestliTable extends AbstractIntegrationTestHarness {

  @Test
  public void testStreamTableJoin() throws Exception {

    List<Profile> received = new LinkedList<>();
    List<EnrichedPageView> joined = new LinkedList<>();

    int count = 10;
    Profile[] profiles = TestTableData.generateProfiles(count);

    MyJoinFn joinFn = new MyJoinFn();

    int partitionCount = 4;
    Map<String, String> configs = getBaseJobConfig();

    configs.put("streams.Profile.samza.system", "test");
    configs.put("streams.Profile.samza.bootstrap", "true");
    configs.put("streams.Profile.source", Base64Serializer.serialize(profiles));
    configs.put("streams.Profile.partitionCount", String.valueOf(partitionCount));

    final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(configs));
    final StreamApplication app = (streamGraph, cfg) -> {

      Table<KV<Integer, PageView>> table = streamGraph.getTable(new RestliTableDescriptor<Integer, PageView>("t1")
          .withFn((key, client) -> new PageView("page1", key)));

      streamGraph.getInputStream("Profile", new NoOpSerde<Profile>())
          .map(m -> {
            received.add(m);
            return m;
          })
          .partitionBy(Profile::getMemberId, v -> v, "p1")
          .join(table, joinFn)
          .sink((m, collector, coordinator) -> joined.add(m));
    };

    runner.run(app);
    runner.waitForFinish();

    assertEquals(received.size(), joined.size());
//    assertEquals(count * partitionCount, joined.size());
    assertTrue(joined.get(0) instanceof EnrichedPageView);
  }

  private Map<String, String> getBaseJobConfig() {
    Map<String, String> configs = new HashMap<>();
    configs.put("systems.test.samza.factory", ArraySystemFactory.class.getName());

    configs.put(JobConfig.JOB_NAME(), "test-table-job");
    configs.put(JobConfig.PROCESSOR_ID(), "1");
    configs.put(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, PassthroughCoordinationUtilsFactory.class.getName());
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
    configs.put(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());

    // For intermediate streams
    configs.put("systems.kafka.samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory");
    configs.put("systems.kafka.producer.bootstrap.servers", bootstrapUrl());
    configs.put("systems.kafka.consumer.zookeeper.connect", zkConnect());
    configs.put("systems.kafka.samza.key.serde", "int");
    configs.put("systems.kafka.samza.msg.serde", "json");
    configs.put("systems.kafka.default.stream.replication.factor", "1");
    configs.put("job.default.system", "kafka");

    configs.put("serializers.registry.int.class", "org.apache.samza.serializers.IntegerSerdeFactory");
    configs.put("serializers.registry.json.class", TestTableData.ProfileJsonSerdeFactory.class.getName());

    return configs;
  }

  private class MyJoinFn implements StreamTableJoinFunction
      <Integer, KV<Integer, Profile>, KV<Integer, PageView>, EnrichedPageView> {

    @Override
    public EnrichedPageView apply(KV<Integer, Profile> message,
        KV<Integer, PageView> record) {
      return record == null ? null : new EnrichedPageView(
          record.getValue().getPageKey(),
          message.getKey(),
          message.getValue().getCompany());
    }

    @Override
    public Integer getMessageKey(KV<Integer, Profile> message) {
      return message.getKey();
    }

    @Override
    public Integer getRecordKey(KV<Integer, PageView> record) {
      return record.getKey();
    }
  }
}
