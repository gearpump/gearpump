/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.kafka.lib

import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.mock.MockitoSugar


import scala.util.Try

class KafkaConfigSpec extends PropSpec with Matchers with MockitoSugar {

  property("kafka conf should be set correctly") {
    val config = KafkaConfig(ConfigFactory.parseResources("kafka.conf"))

    assert(Try(config.getZookeeperConnect).isSuccess)
    assert(Try(config.getConsumerTopics).isSuccess)
    assert(Try(config.getSocketTimeoutMS).isSuccess)
    assert(Try(config.getSocketReceiveBufferBytes).isSuccess)
    assert(Try(config.getFetchMessageMaxBytes).isSuccess)
    assert(Try(config.getClientId).isSuccess)
    assert(Try(config.getConsumerEmitBatchSize).isSuccess)
    assert(Try(config.getFetchSleepMS).isSuccess)
    assert(Try(config.getFetchThreshold).isSuccess)
    assert(Try(config.getProducerTopic).isSuccess)
    assert(Try(config.getProducerEmitBatchSize).isSuccess)
    assert(Try(config.getProducerType).isSuccess)
    assert(Try(config.getRequestRequiredAcks).isSuccess)
    assert(Try(config.getMetadataBrokerList).isSuccess)
    assert(Try(config.getGrouperFactory).isSuccess)
    assert(Try(config.getStorageReplicas).isSuccess)
    assert(Try(config.getMessageDecoder).isSuccess)
    assert(Try(config.getTimeStampFilter).isSuccess)
  }
}
