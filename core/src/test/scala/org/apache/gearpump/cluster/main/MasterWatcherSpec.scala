/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.cluster.main

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestProbe
import com.typesafe.config.Config
import org.apache.gearpump.cluster.TestUtil
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class MasterWatcherSpec extends FlatSpec with Matchers {
  def config: Config = TestUtil.MASTER_CONFIG

  "MasterWatcher" should "kill itself when can not get a quorum" in {
    val system = ActorSystem("ForMasterWatcher", config)

    val actorWatcher = TestProbe()(system)

    val masterWatcher = system.actorOf(Props(classOf[MasterWatcher], "watcher"))
    actorWatcher watch masterWatcher
    actorWatcher.expectTerminated(masterWatcher, 5.seconds)
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }
}
