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

package org.apache.gearpump.experiments.storm

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import backtype.storm.generated.StormTopology
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.experiments.storm.Commands.{GetTopology, Kill, Submit}
import org.apache.gearpump.experiments.storm.GearpumpThriftServer.GearpumpNimbus
import org.apache.gearpump.experiments.storm.util.TopologyUtil
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Future

class GearpumpNimbusSpec extends WordSpec with Matchers with MockitoSugar {

  "GearpumpNimbus" should {
    "submit and kill topology through ClientContext" in {

      implicit val system = ActorSystem("storm-test", TestUtil.DEFAULT_CONFIG)
      implicit val dispatcher = system.dispatcher

      val handler = TestProbe()
      val gearpumpNimbus = new GearpumpNimbus(handler.ref)

      val appId = 0
      val name = "test"
      val uploadedJarLocation = "local"
      val jsonConf = "storm_json_conf"
      val topology = TopologyUtil.getTestTopology

      Future(gearpumpNimbus.submitTopology(name, uploadedJarLocation, jsonConf, topology))
      handler.expectMsgType[Submit]

      Future(gearpumpNimbus.getTopology(name))
      handler.expectMsgType[GetTopology]
      handler.reply(new StormTopology)

      Future(gearpumpNimbus.getUserTopology(name))
      handler.expectMsgType[GetTopology]
      handler.reply(new StormTopology)

      Future(gearpumpNimbus.killTopology(name))
      handler.expectMsgType[Kill]

      system.shutdown()
    }
  }
}