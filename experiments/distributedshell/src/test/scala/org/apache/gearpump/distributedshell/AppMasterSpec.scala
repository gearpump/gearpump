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
import akka.testkit.{TestActorRef, TestProbe}
import org.apache.gearpump.cluster.AppMasterToMaster.{RequestResource, GetAllWorkers, RegisterAppMaster}
import org.apache.gearpump.cluster.AppMasterToWorker.LaunchExecutor
import org.apache.gearpump.cluster.MasterToAppMaster.{ResourceAllocated, WorkerList, AppMasterRegistered}
import org.apache.gearpump.cluster._
import org.apache.gearpump.cluster.master.AppMasterRuntimeInfo
import org.apache.gearpump.cluster.scheduler.{ResourceAllocation, Relaxation, ResourceRequest, Resource}
import org.apache.gearpump.experiments.cluster.ExecutorToAppMaster.ResponsesFromTasks
import org.apache.gearpump.util.ActorSystemBooter.{BindLifeCycle, RegisterActorSystem}
import org.apache.gearpump.util.ActorUtil
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

class AppMasterSpec extends WordSpec with Matchers with BeforeAndAfter{
  implicit val system = ActorSystem("AppMasterSpec", TestUtil.DEFAULT_CONFIG)
  val mockMaster = TestProbe()(system)
  val mockWorker1 = TestProbe()(system)
  val masterProxy = mockMaster.ref
  val appId = 0
  val userName = "test"
  val masterExecutorId = 0
  val workerList = List(1, 2, 3)
  val resource = Resource(1)
  val appJar = None
  val appDescription = Application("app0", "AppMaster", UserConfig.empty)

  "DistributedShell AppMaster" should {
    "launch one ShellTask on each worker" in {
      val appMasterInfo = AppMasterRuntimeInfo(mockWorker1.ref, appId, resource)
      val appMasterContext = AppMasterContext(appId, userName, resource, appJar, masterProxy, appMasterInfo)
      val appMaster = TestActorRef(Props(classOf[AppMaster], appMasterContext, appDescription))
      //When AppMaster started, it will register itself to Master
      mockMaster.expectMsg(RegisterAppMaster(appMaster, appMasterInfo))
      appMaster.tell(AppMasterRegistered(appId, mockMaster.ref), masterProxy)
      //The DistributedShell AppMaster will ask for worker list
      mockMaster.expectMsg(GetAllWorkers)
      mockMaster.reply(WorkerList(workerList))
      //After worker list is ready, DistributedShell AppMaster will request resouce on each worker
      workerList.foreach { workerId =>
        mockMaster.expectMsg(RequestResource(appId, ResourceRequest(Resource(1), workerId, relaxation = Relaxation.SPECIFICWORKER)))
      }
      val resouceAllocated = ResourceAllocated(Array(ResourceAllocation(resource, mockWorker1.ref, 1)))
      appMaster.tell(resouceAllocated, masterProxy)
      mockWorker1.expectMsgClass(classOf[LaunchExecutor])
      mockWorker1.reply(RegisterActorSystem(ActorUtil.getSystemAddress(system).toString))
      val appMasterActor = appMaster.underlying.actor.asInstanceOf[AppMaster]
      assert(appMasterActor.scheduleTaskOnWorker(0).taskClass.equals(classOf[ShellTask].getCanonicalName))
      mockMaster.expectNoMsg()
    }
  }

  "ResponseBuilder" should {
    "aggregate ResponsesFromTasks" in {
      val executorId1 = 1
      val executorId2 = 2
      val responseBuilder = new ResponseBuilder
      val response1 = ResponsesFromTasks(executorId1, List("task1", "task2"))
      val response2 = ResponsesFromTasks(executorId2, List("task3", "task4"))
      val result = responseBuilder.aggregate(response1).aggregate(response2).toString()
      println(result)
      val expected = s"Execute results from executor $executorId1 : \ntask1task2\n" +
        s"Execute results from executor $executorId2 : \ntask3task4\n"
      assert(result == expected)
    }
  }

  after {
    system.shutdown()
  }

}
