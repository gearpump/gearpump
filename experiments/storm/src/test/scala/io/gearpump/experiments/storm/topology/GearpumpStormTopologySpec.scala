/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.experiments.storm.topology

import java.util.{HashMap => JHashMap, Map => JMap}

import backtype.storm.Config
import io.gearpump.experiments.storm.processor.StormProcessor
import io.gearpump.experiments.storm.producer.StormProducer
import io.gearpump.experiments.storm.util.TopologyUtil
import io.gearpump.streaming.MockUtil
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class GearpumpStormTopologySpec extends WordSpec with Matchers with MockitoSugar {
  import io.gearpump.experiments.storm.topology.GearpumpStormTopologySpec._

  "GearpumpStormTopology" should {
    "merge configs with defined priority" in {
      val stormTopology = TopologyUtil.getTestTopology
      val name = "name"
      val sysVal = "sys"
      val sysConfig = newJavaConfig(name, sysVal)
      val appVal = "app"
      val appConfig = newJavaConfig(name, appVal)

      implicit val system = MockUtil.system
      val topology1 = new GearpumpStormTopology("topology1", stormTopology, newEmptyConfig, newEmptyConfig)
      topology1.getStormConfig(Config.TOPOLOGY_NAME) shouldBe "topology1"
      topology1.getStormConfig should not contain name

      val topology2 = new GearpumpStormTopology("topology2", stormTopology, sysConfig, newEmptyConfig)
      topology2.getStormConfig(Config.TOPOLOGY_NAME) shouldBe "topology2"
      topology2.getStormConfig.get(name) shouldBe sysVal

      val topology3 = new GearpumpStormTopology("topology3", stormTopology, sysConfig, appConfig)
      topology3.getStormConfig(Config.TOPOLOGY_NAME) shouldBe "topology3"
      topology3.getStormConfig.get(name) shouldBe appVal

    }

    "create Gearpump processors from Storm topology" in {
      val stormTopology = TopologyUtil.getTestTopology
      implicit val system = MockUtil.system
      val gearpumpStormTopology =
        GearpumpStormTopology("app", stormTopology, null)
      val processors = gearpumpStormTopology.getProcessors
      stormTopology.get_spouts().foreach { case (spoutId, _) =>
        val processor = processors(spoutId)
        processor.taskClass shouldBe classOf[StormProducer]
        processor.description shouldBe spoutId
      }
      stormTopology.get_bolts().foreach { case (boltId, _) =>
        val processor = processors(boltId)
        processor.taskClass shouldBe classOf[StormProcessor]
        processor.description shouldBe boltId
      }
    }

    "get target processors from source id" in {
      val stormTopology = TopologyUtil.getTestTopology
      implicit val system = MockUtil.system
      val sysConfig = new JHashMap[AnyRef, AnyRef]
      val gearpumpStormTopology =
        GearpumpStormTopology("app", stormTopology, null)
      val targets0 = gearpumpStormTopology.getTargets("1")
      targets0 should contain key "3"
      targets0 should contain key "4"
      val targets1 = gearpumpStormTopology.getTargets("2")
      targets1 should contain key "3"
    }
  }

}

object GearpumpStormTopologySpec {
  def newEmptyConfig: JMap[AnyRef, AnyRef] = {
    new JHashMap[AnyRef, AnyRef]
  }

  def newJavaConfig(key: AnyRef, value: AnyRef): JMap[AnyRef, AnyRef] = {
    val config = new JHashMap[AnyRef, AnyRef]
    config.put(key, value)
    config
  }
}
