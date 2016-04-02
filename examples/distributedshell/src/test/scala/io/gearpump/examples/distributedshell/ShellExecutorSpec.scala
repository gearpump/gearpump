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
package io.gearpump.examples.distributedshell

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestProbe
import io.gearpump.WorkerId
import io.gearpump.examples.distributedshell.DistShellAppMaster.ShellCommandResult
import io.gearpump.cluster.appmaster.WorkerInfo
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.cluster.{ExecutorContext, TestUtil, UserConfig}
import DistShellAppMaster.{ShellCommand, ShellCommandResult}
import org.scalatest.{Matchers, WordSpec}

import scala.sys.process._
import scala.util.{Failure, Success, Try}

class ShellExecutorSpec extends WordSpec with Matchers {

  "ShellExecutor" should {
    "execute the shell command and return the result" in {
      val executorId = 1
      val workerId = WorkerId(2, 0L)
      val appId = 0
      val appName = "app"
      val resource = Resource(1)
      implicit val system = ActorSystem("ShellExecutor", TestUtil.DEFAULT_CONFIG)
      val mockMaster = TestProbe()(system)
      val worker = TestProbe()
      val workerInfo = WorkerInfo(workerId, worker.ref)
      val executorContext = ExecutorContext(executorId, workerInfo, appId, appName, mockMaster.ref, resource)
      val executor = system.actorOf(Props(classOf[ShellExecutor], executorContext, UserConfig.empty))

      val process = Try(s"ls /" !!)
      val result = process match {
        case Success(msg) => msg
        case Failure(ex) => ex.getMessage
      }
      executor.tell(ShellCommand("ls /"), mockMaster.ref)
      assert(mockMaster.receiveN(1).head.asInstanceOf[ShellCommandResult].equals(
        ShellCommandResult(executorId, result)))

      system.shutdown()
      system.awaitTermination()
    }
  }
}
