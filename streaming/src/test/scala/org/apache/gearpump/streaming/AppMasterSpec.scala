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
package org.apache.gearpump.streaming

import akka.actor.{PoisonPill, ActorRef, Props}
import akka.testkit.{EventFilter, TestProbe, TestActorRef}
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.AppMasterToMaster._
import org.apache.gearpump.cluster.AppMasterToWorker.LaunchExecutor
import org.apache.gearpump.cluster.ClientToMaster.ShutdownApplication
import org.apache.gearpump.cluster.MasterToAppMaster.{ResourceAllocated, AppMasterRegistered}
import org.apache.gearpump.cluster.WorkerToAppMaster.ExecutorLaunchRejected
import org.apache.gearpump.cluster._
import org.apache.gearpump.cluster.master.AppMasterRuntimeInfo
import org.apache.gearpump.cluster.scheduler.{Relaxation, ResourceAllocation, ResourceRequest, Resource}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.util.ActorSystemBooter.RegisterActorSystem
import org.apache.gearpump.util.{ActorUtil, Graph}
import org.scalatest._

import org.apache.gearpump.util.Graph._
import scala.concurrent.duration._

class AppMasterSpec extends WordSpec with Matchers with BeforeAndAfterEach with MasterHarness {
  override def config = TestUtil.DEFAULT_CONFIG

  var appMaster: TestActorRef[AppMaster] = null

  val appId = 0
  val masterExecutorId = 0
  val workerId = 1
  val resource = Resource(1)
  val taskDescription1 = TaskDescription(classOf[TaskActorA].getName, 2)
  val taskDescription2 = TaskDescription(classOf[TaskActorB].getName, 2)
  var conf: UserConfig = null
  var mockProxy: TestProbe = null
  var mockMaster: TestProbe = null
  var mockWorker: TestProbe = null
  var appDescription: Application = null
  var appMasterContext: AppMasterContext = null
  var appMasterRuntimeInfo: AppMasterRuntimeInfo = null

  override def beforeEach() = {
    startActorSystem()
    mockProxy = TestProbe()(getActorSystem)
    mockMaster = TestProbe()(getActorSystem)
    mockWorker = TestProbe()(getActorSystem)
    mockMaster.ignoreMsg(ignoreSaveAppData)
    appMasterRuntimeInfo = AppMasterRuntimeInfo(mockWorker.ref, appId, Resource(1))

    implicit val system = getActorSystem
    conf = UserConfig.empty.withValue(AppMasterSpec.MASTER, mockMaster.ref)
    appMasterContext = AppMasterContext(appId, "test", resource, None, mockProxy.ref, appMasterRuntimeInfo)
    appDescription = TestUtil.dummyApp
    appMaster = TestActorRef[AppMaster](Props(classOf[AppMaster], appMasterContext, appDescription))(getActorSystem)

    mockProxy.expectMsg(RegisterAppMaster(appMaster, appMasterRuntimeInfo))
    mockProxy.reply(AppMasterRegistered(appId, mockMaster.ref))
    mockMaster.expectMsg(GetAppData(appId, "startClock"))
    mockMaster.reply(GetAppDataResult("startClock", 0L))
    mockMaster.expectMsg(RequestResource(appId, ResourceRequest(Resource(4))))
    Thread.sleep(1000)
  }

  override def afterEach() = {
    shutdownActorSystem()
  }

  "AppMaster" should {
    "kill it self when allocate resource time out" in {
      mockMaster.reply(ResourceAllocated(Array(ResourceAllocation(Resource(2), mockWorker.ref, workerId))))
      mockMaster.expectMsg(31 seconds, ShutdownApplication(appId))
    }

    "launch executor and task properly" in {
      mockMaster.reply(ResourceAllocated(Array(ResourceAllocation(Resource(4), mockWorker.ref, workerId))))
      mockWorker.expectMsgClass(classOf[LaunchExecutor])
      mockWorker.reply(RegisterActorSystem(ActorUtil.getSystemAddress(getActorSystem).toString))
      for (i <- 1 to 4) {
        mockMaster.expectMsg(10 seconds, AppMasterSpec.TaskStarted)
      }
      appMaster ! ExecutorLaunchRejected("", resource)
      mockMaster.expectMsg(RequestResource(appId, ResourceRequest(resource)))

      appMaster.tell(UpdateClock(TaskId(0, 0), 1024), mockProxy.ref)
      mockProxy.expectMsg(ClockUpdated(1024))
      appMaster.tell(GetLatestMinClock, mockProxy.ref)
      mockProxy.expectMsg(LatestMinClock(1024))

      val executorId = masterExecutorId + 1
      appMaster.children.foreach { child =>
        if(child.path.name.equals(executorId.toString)) {
          child ! PoisonPill
        }
      }
      mockMaster.expectMsg(RequestResource(appId, ResourceRequest(Resource(4), relaxation = Relaxation.ONEWORKER)))
    }

    "find a new master when lost connection with master" in {
      println(config.getList("akka.loggers"))
      mockMaster.ref ! PoisonPill
      mockProxy.expectMsg(RegisterAppMaster(appMaster, appMasterRuntimeInfo))
    }
  }

  def ignoreSaveAppData: PartialFunction[Any, Boolean] = {
    case msg: SaveAppData => true
  }
}

object AppMasterSpec {
  val MASTER = "master"
  case object TaskStarted
}

class TaskActorA(taskContext : TaskContext, userConf : UserConfig) extends TaskActor(taskContext, userConf) {

  val master = userConf.getValue[ActorRef](AppMasterSpec.MASTER).get
  override def onStart(startTime: StartTime): Unit = master ! AppMasterSpec.TaskStarted

  override def onNext(msg: Message): Unit = {}
}

class TaskActorB(taskContext : TaskContext, userConf : UserConfig) extends TaskActor(taskContext, userConf) {

  val master = userConf.getValue[ActorRef](AppMasterSpec.MASTER).get
  override def onStart(startTime: StartTime): Unit = master ! AppMasterSpec.TaskStarted

  override def onNext(msg: Message): Unit = {}
}