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

package org.apache.gearpump.streaming.examples.kafka

import org.apache.gearpump.streaming.task.TaskId
import kafka.common.TopicAndPartition

/**
 * default grouper groups TopicAndPartitions among Spouts by partitions
 *
 * e.g. given 2 topics (topicA with 2 partitions and topicB with 3 partitions) and
 * 2 spouts (spout0 and spout1)
 *
 * spout0 gets (topicA, partition1), (topicB, partition1) and (topicA, partition3)
 * spout1 gets (topicA, partition2), (topicB, partition2)
 */
class KafkaDefaultGrouper {
  def group(topicAndPartitions: List[TopicAndPartition],
                     taskNum: Int, taskId: TaskId): List[TopicAndPartition] = {
    val taskToTopicAndPartitions = topicAndPartitions.groupBy(tp => tp.partition % taskNum).map(params =>
      (TaskId(taskId.groupId, params._1), params._2.toList)
    )
    taskToTopicAndPartitions(taskId)
  }
}
