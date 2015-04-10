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

import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.experiments.storm.util.TopologyUtil
import org.apache.gearpump.streaming.StreamApplication
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.mock.MockitoSugar

class GearpumpNimbusSpec extends WordSpec with Matchers with MockitoSugar {

  "GearpumpNimbus" should {
    "submit and kill topology through ClientContext" in {
      val clientContext = mock[ClientContext]
      val gearpumpNimbus = new GearpumpNimbus(clientContext)

      val appId = 0
      val name = "test"
      val uploadedJarLocation = "local"
      val jsonConf = "storm_json_conf"
      val topology = TopologyUtil.getTestTopology

      when(clientContext.submit(any(classOf[StreamApplication]))).thenReturn(appId)

      gearpumpNimbus.submitTopology(name, uploadedJarLocation, jsonConf, topology)
      verify(clientContext).submit(any(classOf[StreamApplication]))

      gearpumpNimbus.getTopology(name) shouldBe topology
      gearpumpNimbus.getUserTopology(name) shouldBe topology

      gearpumpNimbus.killTopology(name)
      verify(clientContext).shutdown(appId)

      intercept[RuntimeException] {
        gearpumpNimbus.killTopology(name)
      }
    }

  }

}
