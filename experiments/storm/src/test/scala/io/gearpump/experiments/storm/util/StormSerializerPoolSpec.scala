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

import java.util.{HashMap => JHashMap, List => JList, Map => JMap}

import akka.actor.ExtendedActorSystem
import backtype.storm.utils.Utils
import com.esotericsoftware.kryo.Kryo
import io.gearpump.cluster.UserConfig
import io.gearpump.experiments.storm.topology.GearpumpTuple
import io.gearpump.experiments.storm.util.StormConstants._
import io.gearpump.streaming.MockUtil
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import scala.collection.JavaConverters._

class StormSerializerPoolSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("StormSerializerPool should create and manage StormSerializer") {
    val taskContext = MockUtil.mockTaskContext
    val serializerPool = new StormSerializationFramework
    val system = taskContext.system.asInstanceOf[ExtendedActorSystem]
    implicit val actorSystem = system
    val stormConfig = Utils.readDefaultConfig.asInstanceOf[JMap[AnyRef, AnyRef]]
    val config = UserConfig.empty.withValue[JMap[AnyRef, AnyRef]](STORM_CONFIG, stormConfig)
    serializerPool.init(system, config)
    serializerPool.get shouldBe a [StormSerializer]
  }

  property("StormSerializer should serialize and deserialize GearpumpTuple") {
    val tupleGen = for {
      values <- Gen.listOf[String](Gen.alphaStr).map(_.asJava.asInstanceOf[JList[AnyRef]])
      sourceTaskId <- Gen.chooseNum[Int](0, Int.MaxValue)
      sourceStreamId <- Gen.alphaStr
    } yield new GearpumpTuple(values, new Integer(sourceTaskId), sourceStreamId, null)

    val kryo = new Kryo
    forAll(tupleGen) { (tuple: GearpumpTuple) =>
      val serializer = new StormSerializer(kryo)
      serializer.deserialize(serializer.serialize(tuple)) shouldBe tuple
    }
  }
}
