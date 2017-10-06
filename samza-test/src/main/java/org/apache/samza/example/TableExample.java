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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.operators.RecordTable;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.IntegerSerdeFactory;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.standalone.PassthroughCoordinationUtilsFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.storage.kv.RocksDbTableDescriptor;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


public class TableExample {

  public static class PageView implements Serializable {
    @JsonProperty("pageKey")
    final String pageKey;
    @JsonProperty("memberId")
    final int memberId;

    @JsonProperty("pageKey")
    public String getPageKey() {
      return pageKey;
    }

    @JsonProperty("memberId")
    public int getMemberId() {
      return memberId;
    }

    @JsonCreator
    public PageView(@JsonProperty("memberId") int memberId, @JsonProperty("pageKey") String pageKey) {
      this.pageKey = pageKey;
      this.memberId = memberId;
    }
  }

  public static class Profile implements Serializable {
    @JsonProperty("memberId")
    final int memberId;

    @JsonProperty("company")
    final String company;

    @JsonProperty("memberId")
    public int getMemberId() {
      return memberId;
    }

    @JsonProperty("company")
    public String getCompany() {
      return company;
    }

    @JsonCreator
    public Profile(@JsonProperty("memberId") int memberId, @JsonProperty("company") String company) {
      this.memberId = memberId;
      this.company = company;
    }
  }

  public static class EnrichedPageView extends PageView {

    @JsonProperty("company")
    final String company;

    @JsonProperty("company")
    public String getCompany() {
      return company;
    }

    @JsonCreator
    public EnrichedPageView(
        @JsonProperty("memberId") int memberId,
        @JsonProperty("pageKey") String pageKey,
        @JsonProperty("company") String company) {
      super(memberId, pageKey);
      this.company = company;
    }
  }

  public static class PageViewJsonSerdeFactory implements SerdeFactory<PageView> {
    @Override public Serde<PageView> getSerde(String name, Config config) {
      return new PageViewJsonSerde();
    }
  }

  public static class ProfileJsonSerdeFactory implements SerdeFactory<Profile> {
    @Override public Serde<Profile> getSerde(String name, Config config) {
      return new ProfileJsonSerde();
    }
  }

  public static class PageViewJsonSerde implements Serde<PageView> {
    ObjectMapper mapper = new ObjectMapper();

    @Override
    public PageView fromBytes(byte[] bytes) {
      try {
        return mapper.readValue(new String(bytes, "UTF-8"), new TypeReference<PageView>() { });
      } catch (Exception e) {
        throw new SamzaException(e);
      }
    }

    @Override
    public byte[] toBytes(PageView pv) {

      try {
        return mapper.writeValueAsString(pv).getBytes("UTF-8");
      } catch (Exception e) {
        throw new SamzaException(e);
      }
    }
  }

  public static class ProfileJsonSerde implements Serde<Profile> {
    ObjectMapper mapper = new ObjectMapper();

    @Override
    public Profile fromBytes(byte[] bytes) {
      try {
        return mapper.readValue(new String(bytes, "UTF-8"), new TypeReference<Profile>() { });
      } catch (Exception e) {
        throw new SamzaException(e);
      }
    }

    @Override
    public byte[] toBytes(Profile p) {
      try {
        return mapper.writeValueAsString(p).getBytes("UTF-8");
      } catch (Exception e) {
        throw new SamzaException(e);
      }
    }
  }

  static public class MyTableJoinFn implements StreamTableJoinFunction<Integer, PageView, Profile, EnrichedPageView> {
    @Override
    public EnrichedPageView apply(PageView pv, Profile p) {
      return new EnrichedPageView(pv.getMemberId(), pv.getPageKey(), p.getCompany());
    }
    @Override
    public Integer getFirstKey(PageView pv) {
      return pv.getMemberId();
    }
  }

  public class MyStreamJoinFn implements JoinFunction<Integer, PageView, Profile, EnrichedPageView> {
    @Override
    public EnrichedPageView apply(PageView pv, Profile p) {
      return new EnrichedPageView(pv.getMemberId(), pv.getPageKey(), p.getCompany());
    }
    @Override
    public Integer getFirstKey(PageView message) {
      return message.getMemberId();
    }

    @Override
    public Integer getSecondKey(Profile message) {
      return message.getMemberId();
    }
  }

  final static private PageView[] PAGEVIEWS = new PageView[] {
      new PageView(1, "inbox"),
      new PageView(2, "home"),
      new PageView(3, "home"),
      new PageView(4, "search"),
      new PageView(5, "inbox"),
      new PageView(6, "search")
  };

  final static private Profile[] PROFILES = new Profile[] {
      new Profile(1, "MSFT"),
      new Profile(2, "CSCO"),
      new Profile(3, "FB"),
      new Profile(4, "GOOG"),
      new Profile(5, "MSFT"),
      new Profile(6, "GOOG")
  };

  public static void main(String[] args) throws Exception {
    Map<String, String> configs = new HashMap<>();
    configs.put("systems.test.samza.factory", ArraySystemFactory.class.getName());
    configs.put("streams.PageView.samza.system", "test");
    configs.put("streams.PageView.source", Base64Serializer.serialize(PAGEVIEWS));
    configs.put("streams.Profile.samza.system", "test");
    configs.put("streams.Profile.source", Base64Serializer.serialize(PROFILES));
    configs.put("streams.Profile.samza.bootstrap", "true");

    configs.put(JobConfig.JOB_NAME(), "test-table-job");
    configs.put(JobConfig.PROCESSOR_ID(), "1");
    configs.put(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, PassthroughCoordinationUtilsFactory.class.getName());
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
    configs.put(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());

    configs.put("systems.queuing.samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory");
    configs.put("systems.queuing.producer.bootstrap.servers", "ltx1-kafka-kafka-queuing-vip.stg.linkedin.com:10251");
    configs.put("systems.queuing.consumer.zookeeper.connect", "zk-ltx1-kafka.stg.linkedin.com:12913/kafka-queuing");
    configs.put("systems.queuing.samza.key.serde", "int");
    configs.put("systems.queuing.samza.msg.serde", "json");
    configs.put("job.default.system", "queuing");

    configs.put("serializers.registry.int.class", IntegerSerdeFactory.class.getName());
    configs.put("serializers.registry.json.class", PageViewJsonSerdeFactory.class.getName());

    final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(configs));
    final StreamApplication app = (streamGraph, cfg) -> {

      RecordTable<Integer, Profile> table = streamGraph.getRecordTable(
              new RocksDbTableDescriptor.Factory<Integer, Profile>().getTableDescriptor("t1")
          .withKeySerde(new IntegerSerde())
          .withValueSerde(new ProfileJsonSerde())
          .withWriteBatchSize(500));

      streamGraph.getInputStream("Profile", (k, v) -> (Profile) v)
          .filter(m -> m.getMemberId() != 2)
          .writeTo(table, m -> m.getMemberId(), m -> m);

      streamGraph.getInputStream("PageView", (k, v) -> (PageView) v)
          .partitionBy(PageView::getMemberId)
          .join(table, new MyTableJoinFn())
          .sink((m, collector, coordinator) -> System.out.println(String.format(
              "Received %d, %s, %s",
              m.getMemberId(), m.getPageKey(), m.getCompany())));
    };

    runner.run(app);
    runner.waitForFinish();
  }
}
