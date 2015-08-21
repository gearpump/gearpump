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

package io.gearpump.experiments.storm.util

import java.util.{List => JList, Map => JMap}

import backtype.storm.generated.StormTopology
import backtype.storm.tuple.{Fields, Tuple, TupleImpl}

private[storm] case class StormTuple(
    tuple: JList[AnyRef],
    componentId: String,
    streamId: String,
    stormTaskId: Int,
    targetPartitions: Map[String, Int]) {
  def toTuple(topology: StormTopology, stormConfig: JMap[_, _],
      taskToComponent: JMap[Integer, String], componentToTasks: JMap[String, JList[Integer]],
      componentToStreamFields: JMap[String, JMap[String, Fields]]): Tuple = {
    val topologyContext = StormUtil.buildTopologyContext(topology, stormConfig, taskToComponent, componentToTasks,
      componentToStreamFields, componentId, stormTaskId)
    new TupleImpl(topologyContext, tuple, stormTaskId, streamId, null)
  }
}



