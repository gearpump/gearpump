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

package io.gearpump.streaming.kafka.lib.grouper

import kafka.common.TopicAndPartition
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class KafkaDefaultGrouperSpec extends PropSpec with PropertyChecks with Matchers {
  property("KafkaDefaultGrouper should group TopicAndPartitions in a round-robin way") {
    forAll(Gen.posNum[Int], Gen.posNum[Int], Gen.posNum[Int]) {
      (topicNum: Int, partitionNum: Int, taskNum: Int) => {
        val topicAndPartitions = for {
          t <- 0.until(topicNum)
          p <- 0.until(partitionNum)
        } yield TopicAndPartition("topic" + t, p)
        0.until(taskNum).foreach { taskIndex =>
          val grouper = new KafkaDefaultGrouper
          grouper.group(taskNum, taskIndex, topicAndPartitions.toArray).forall(
            tp => topicAndPartitions.indexOf(tp) % taskNum == taskIndex)
        }
      }
    }
  }

}
