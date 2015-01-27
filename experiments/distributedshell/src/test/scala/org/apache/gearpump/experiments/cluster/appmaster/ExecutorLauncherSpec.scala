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
package org.apache.gearpump.experiments.cluster.appmaster

import akka.actor.{Props, ActorSystem}
import akka.testkit.TestProbe
import org.apache.gearpump.cluster.AppMasterToWorker.LaunchExecutor
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.cluster.{UserConfig, ExecutorJVMConfig, ExecutorContext, TestUtil}
import org.apache.gearpump.experiments.cluster.executor.DefaultExecutor
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._
import scala.language.postfixOps

class ExecutorLauncherSpec extends WordSpec with Matchers {

  "ExecutorLauncher" should {
    "kill it self when actor system registering time out" in {
      val executorId = 1
      val workerId = 2
      val appId = 0
      val resource = Resource(2)
      val executorClass = classOf[DefaultExecutor]
      val system = ActorSystem("ExecutorLauncher", TestUtil.DEFAULT_CONFIG)
      val mockWorker = TestProbe()(system)
      val mockMaster = TestProbe()(system)
      val executorContext = ExecutorContext(executorId, workerId, appId, mockMaster.ref, resource)
      val jvmConfig = ExecutorJVMConfig(Array.empty[String], Array.empty[String], "", Array.empty[String], None, "test")
      val mockLaunch = LaunchExecutor(appId, executorId, resource, jvmConfig)
      val executorLauncher = system.actorOf(Props(classOf[ExecutorLauncher], executorClass, mockWorker.ref, mockLaunch, executorContext, UserConfig.empty))
      mockWorker.expectMsg(mockLaunch)

      mockMaster watch executorLauncher
      mockMaster.expectTerminated(executorLauncher, 16 seconds)
      system.shutdown()
    }
  }
}
