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
package org.apache.gearpump.services

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.cluster.TestUtil.MiniCluster
import org.apache.gearpump.streaming.StreamingTestUtil
import spray.can.Http

import scala.util.Try


object RestTestUtil {

  def startRestServices:Try[RestTest] = Try(new RestTest().startRestServices)

  class RestTest {
    val miniCluster:MiniCluster = TestUtil.startMiniCluster
    val appId = 0
    val master = miniCluster.mockMaster
    StreamingTestUtil.startAppMaster(miniCluster, appId)
    implicit val system = ActorSystem("Rest", TestUtil.DEFAULT_CONFIG)
    private implicit val timeout = Timeout(5, TimeUnit.SECONDS)

    def startRestServices = {
      RestServices.start(master)
      this
    }

    def shutdown() = {
      miniCluster.shutDown()
      IO(Http) ? Http.CloseAll
      system.shutdown()
    }
  }

}
