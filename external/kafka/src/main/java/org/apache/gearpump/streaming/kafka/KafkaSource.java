/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.streaming.kafka;

import kafka.common.TopicAndPartition;
import org.apache.gearpump.streaming.kafka.lib.source.AbstractKafkaSource;
import org.apache.gearpump.streaming.kafka.lib.util.KafkaClient;
import org.apache.gearpump.streaming.kafka.lib.source.consumer.FetchThread;
import org.apache.gearpump.streaming.kafka.util.KafkaConfig;
import org.apache.gearpump.streaming.transaction.api.CheckpointStore;
import org.apache.gearpump.streaming.transaction.api.TimeReplayableSource;

import java.util.Properties;

/**
 * USER API for kafka source connector.
 * Please refer to {@link AbstractKafkaSource} for detailed descriptions and implementations.
 */
public class KafkaSource extends AbstractKafkaSource implements TimeReplayableSource {

  public KafkaSource(String topic, Properties properties) {
    super(topic, properties);
  }

  // constructor for tests
  KafkaSource(String topic, Properties properties,
      KafkaConfig.KafkaConfigFactory configFactory,
      KafkaClient.KafkaClientFactory clientFactory,
      FetchThread.FetchThreadFactory threadFactory) {
    super(topic, properties, configFactory, clientFactory, threadFactory);
  }

  /**
   * for tests only
   */
  protected void addPartitionAndStore(TopicAndPartition tp, CheckpointStore store) {
    addCheckpointStore(tp, store);
  }
}
