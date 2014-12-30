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
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.cluster.task.TaskContext
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import sys.process._

class ShellTaskSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll  {

  val mockMaster = TestProbe()

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "The ShellTask" should {
    "execute the shell command and return the result" in {
      val taskContext = TaskContext(0, 0, mockMaster.ref)
      val shellTask = system.actorOf(Props(classOf[ShellTask], taskContext, UserConfig.empty))
      shellTask ! ShellCommand("ls", "/")
      val actualResult = "ls /" !!

      expectMsg(actualResult)
    }
  }
}
