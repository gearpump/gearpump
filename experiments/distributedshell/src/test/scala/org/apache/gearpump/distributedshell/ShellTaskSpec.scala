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
package org.apache.gearpump.distributedshell

import akka.actor.{Props, ActorSystem}
import akka.testkit.TestProbe
import org.apache.gearpump.cluster.{TestUtil, UserConfig}
import org.apache.gearpump.experiments.cluster.task.TaskContext
import org.scalatest.{WordSpec, Matchers}
import sys.process._

class ShellTaskSpec extends WordSpec with Matchers {

  "The ShellTask" should {
    "execute the shell command and return the result" in {
      val system = ActorSystem("ShellTask", TestUtil.DEFAULT_CONFIG)
      val mockMaster = TestProbe()(system)
      val taskContext = TaskContext(0, 0, mockMaster.ref)
      val shellTask = system.actorOf(Props(classOf[ShellTask], taskContext, UserConfig.empty))
      shellTask.tell(ShellCommand("ls", "/"), mockMaster.ref)

      mockMaster.expectMsgType[String]
      system.shutdown()
    }
  }
}
