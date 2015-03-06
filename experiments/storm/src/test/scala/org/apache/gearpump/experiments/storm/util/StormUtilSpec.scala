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

package org.apache.gearpump.experiments.storm.util

import akka.actor.ActorSystem
import backtype.storm.generated.StormTopology
import org.apache.gearpump.cluster.UserConfig
import org.scalatest.{Matchers, PropSpec}

class StormUtilSpec extends PropSpec with Matchers {

  implicit val system = ActorSystem("test")

  property("get storm topology through user config") {
    val topology = TopologyUtil.getTestTopology
    val config = UserConfig.empty.withValue[StormTopology](StormUtil.TOPOLOGY, topology)
    StormUtil.getTopology(config) shouldBe topology
  }

  property("get processor to component mapping through user config") {
    val topology = TopologyUtil.getTestTopology
    val graphBuilder = GraphBuilder(topology)
    graphBuilder.build()
    val processorToComponent = graphBuilder.getProcessorToComponent
    val config = UserConfig.empty.withValue[List[(Int, String)]](
      StormUtil.PROCESSOR_TO_COMPONENT, processorToComponent.toList)
    StormUtil.getProcessorToComponent(config) shouldBe processorToComponent
  }
}
