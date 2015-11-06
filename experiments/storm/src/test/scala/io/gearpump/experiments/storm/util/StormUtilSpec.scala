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

package io.gearpump.experiments.storm.util

import java.lang.{Boolean => JBoolean, Long => JLong}
import java.util.{HashMap => JHashMap, Map => JMap}

import backtype.storm.generated.StormTopology
import io.gearpump.cluster.UserConfig
import io.gearpump.experiments.storm.topology.GearpumpStormComponent.{GearpumpBolt, GearpumpSpout}
import io.gearpump.experiments.storm.util.StormConstants._
import io.gearpump.experiments.storm.util.StormUtil._
import io.gearpump.streaming.MockUtil
import io.gearpump.streaming.task.TaskId
import org.json.simple.JSONValue
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class StormUtilSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {


  property("convert Storm task ids to gearpump TaskIds and back") {
    val idGen = Gen.chooseNum[Int](0, Int.MaxValue)
    forAll(idGen) { (stormTaskId: Int) =>
      gearpumpTaskIdToStorm(stormTaskIdToGearpump(stormTaskId)) shouldBe stormTaskId
    }

    val processorIdGen = Gen.chooseNum[Int](0, Int.MaxValue >> 16)
    val indexGen = Gen.chooseNum[Int](0, Int.MaxValue >> 16)
    forAll(processorIdGen, indexGen) { (processorId: Int, index: Int) =>
      val taskId = TaskId(processorId, index)
      stormTaskIdToGearpump(gearpumpTaskIdToStorm(taskId)) shouldBe taskId
    }
  }

  property("get GearpumpStormComponent from user config") {
    val taskContext = MockUtil.mockTaskContext
    val topology = TopologyUtil.getTestTopology
    implicit val actorSystem = taskContext.system
    val userConfig = UserConfig.empty
          .withValue[StormTopology](STORM_TOPOLOGY, topology)
          .withValue[JMap[AnyRef, AnyRef]](STORM_CONFIG, new JHashMap[AnyRef, AnyRef])
    topology.get_spouts foreach { case (spoutId, _) =>
      val config = userConfig.withString(STORM_COMPONENT, spoutId)
      val component = getGearpumpStormComponent(taskContext, config)(taskContext.system)
      component shouldBe a [GearpumpSpout]
    }
    topology.get_bolts foreach { case (boltId, _) =>
      val config = userConfig.withString(STORM_COMPONENT, boltId)
      val component = getGearpumpStormComponent(taskContext, config)(taskContext.system)
      component shouldBe a [GearpumpBolt]
    }
  }

  property("parse json to map") {
    val mapGen = Gen.listOf[String](Gen.alphaStr)
        .map(_.map(s => (s, s)).toMap.asJava.asInstanceOf[JMap[AnyRef, AnyRef]])

    forAll(mapGen) { (map: JMap[AnyRef, AnyRef]) =>
      parseJsonStringToMap(JSONValue.toJSONString(map)) shouldBe map
    }

    val invalidJsonGen: Gen[String] = Gen.oneOf(null, "", "1")
    forAll(invalidJsonGen) { (invalidJson: String) =>
      val map = parseJsonStringToMap(invalidJson)
      map shouldBe empty
      map shouldBe a [JMap[_, _]]
    }
  }

  property("get int from config") {
    val name = "int"
    val conf: JMap[AnyRef, AnyRef] = new JHashMap[AnyRef, AnyRef]
    getInt(conf, name) shouldBe None
    conf.put(name, null)
    getInt(conf, name) shouldBe None

    forAll(Gen.chooseNum[Int](Int.MinValue, Int.MaxValue)) { (int: Int) =>
      conf.put(name, new Integer(int))
      getInt(conf, name) shouldBe Some(int)
    }

    forAll(Gen.chooseNum[Long](Int.MinValue, Int.MaxValue)) { (long: Long) =>
      conf.put(name, new JLong(long))
      getInt(conf, name) shouldBe Some(long)
    }

    forAll(Gen.alphaStr) { (s: String) =>
      conf.put(name, s)
      an [IllegalArgumentException] should be thrownBy getInt(conf, name)
    }
  }

  property("get boolean from config") {
    val name = "boolean"
    val conf: JMap[AnyRef, AnyRef] = new JHashMap[AnyRef, AnyRef]
    getBoolean(conf, name) shouldBe None
    conf.put(name, null)
    getBoolean(conf, name) shouldBe None

    forAll(Gen.oneOf(true, false)) { (boolean: Boolean) =>
      conf.put(name, new JBoolean(boolean))
      getBoolean(conf, name) shouldBe Some(boolean)
    }

    forAll(Gen.alphaStr) { (s: String) =>
      conf.put(name, s)
      an [IllegalArgumentException] should be thrownBy getBoolean(conf, name)
    }
  }

  property("mod should be correct") {
    mod(10, 5) shouldBe 0
    mod(10, 6) shouldBe 4
    mod(10, -3) shouldBe -2
    mod(-2, 5) shouldBe 3
    mod(-1, -2) shouldBe -1
  }
}
