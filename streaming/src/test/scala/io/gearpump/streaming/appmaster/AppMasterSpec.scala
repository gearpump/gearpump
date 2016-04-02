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
package io.gearpump.streaming.appmaster

import akka.actor.{ActorRef, Props}
import akka.testkit.{TestActorRef, TestProbe}
import io.gearpump.{WorkerId, Message}
import io.gearpump.cluster.AppMasterToMaster._
import io.gearpump.cluster.AppMasterToWorker.LaunchExecutor
import io.gearpump.cluster.ClientToMaster.ShutdownApplication
import io.gearpump.cluster.MasterToAppMaster.{AppMasterRegistered, ResourceAllocated}
import io.gearpump.cluster.WorkerToAppMaster.ExecutorLaunchRejected
import io.gearpump.cluster._
import io.gearpump.cluster.appmaster.{AppMasterRuntimeEnvironment, AppMasterRuntimeInfo}
import io.gearpump.cluster.master.MasterProxy
import io.gearpump.cluster.scheduler.{Resource, ResourceAllocation, ResourceRequest}
import io.gearpump.jarstore.FilePath
import io.gearpump.partitioner.HashPartitioner
import io.gearpump.streaming.task.{StartTime, TaskContext, _}
import io.gearpump.streaming.{Processor, StreamApplication}
import io.gearpump.util.Graph
import io.gearpump.util.Graph._
import org.scalatest._

import scala.concurrent.duration._
import scala.language.postfixOps

class AppMasterSpec extends WordSpec with Matchers with BeforeAndAfterEach with MasterHarness {
  override def config = TestUtil.DEFAULT_CONFIG

  var appMaster: ActorRef = null

  val appId = 0
  val workerId = WorkerId(1, 0L)
  val resource = Resource(1)
  val taskDescription1 = Processor[TaskA](2)
  val taskDescription2 = Processor[TaskB](2)
  val partitioner = new HashPartitioner
  var conf: UserConfig = null

  var mockTask: TestProbe = null

  var mockMaster: TestProbe = null
  var mockMasterProxy: ActorRef = null

  var mockWorker: TestProbe = null
  var appDescription: AppDescription = null
  var appMasterContext: AppMasterContext = null
  var appMasterRuntimeInfo: AppMasterRuntimeInfo = null

  override def beforeEach() = {
    startActorSystem()

    mockTask = TestProbe()(getActorSystem)

    mockMaster = TestProbe()(getActorSystem)
    mockWorker = TestProbe()(getActorSystem)
    mockMaster.ignoreMsg(ignoreSaveAppData)
    appMasterRuntimeInfo = AppMasterRuntimeInfo(appId, appName = appId.toString)

    implicit val system = getActorSystem
    conf = UserConfig.empty.withValue(AppMasterSpec.MASTER, mockMaster.ref)
    val mockJar = AppJar("for_test", FilePath("path"))
    appMasterContext = AppMasterContext(appId, "test", resource, null, Some(mockJar), mockMaster.ref, appMasterRuntimeInfo)
    val graph = Graph(taskDescription1 ~ partitioner ~> taskDescription2)
    val streamApp = StreamApplication("test", graph, conf)
    appDescription = Application.ApplicationToAppDescription(streamApp)
    import scala.concurrent.duration._
    mockMasterProxy = getActorSystem.actorOf(
      Props(new MasterProxy(List(mockMaster.ref.path), 30 seconds)), AppMasterSpec.MOCK_MASTER_PROXY)
    TestActorRef[AppMaster](
      AppMasterRuntimeEnvironment.props(List(mockMasterProxy.path), appDescription, appMasterContext))(getActorSystem)

    val registerAppMaster = mockMaster.receiveOne(15 seconds)
    assert(registerAppMaster.isInstanceOf[RegisterAppMaster])
    appMaster = registerAppMaster.asInstanceOf[RegisterAppMaster].appMaster

    mockMaster.reply(AppMasterRegistered(appId))
    mockMaster.expectMsg(15 seconds, GetAppData(appId, "DAG"))
    mockMaster.reply(GetAppDataResult("DAG", null))
    mockMaster.expectMsg(15 seconds, GetAppData(appId, "startClock"))

    mockMaster.reply(GetAppDataResult("startClock", 0L))

    mockMaster.expectMsg(15 seconds, RequestResource(appId, ResourceRequest(Resource(4), workerId = WorkerId.unspecified)))
  }

  override def afterEach() = {
    shutdownActorSystem()
  }

  "AppMaster" should {
    "kill it self when allocate resource time out" in {
      mockMaster.reply(ResourceAllocated(Array(ResourceAllocation(Resource(2), mockWorker.ref, workerId))))
      mockMaster.expectMsg(60 seconds, ShutdownApplication(appId))
    }

    "reschedule the resource when the worker reject to start executor" in {
      val resource = Resource(4)
      mockMaster.reply(ResourceAllocated(Array(ResourceAllocation(resource, mockWorker.ref, workerId))))
      mockWorker.expectMsgClass(classOf[LaunchExecutor])
      mockWorker.reply(ExecutorLaunchRejected(""))
      mockMaster.expectMsg(RequestResource(appId, ResourceRequest(resource, WorkerId.unspecified)))
    }

    "find a new master when lost connection with master" in {
      println(config.getList("akka.loggers"))

      val watcher = TestProbe()(getActorSystem)
      watcher.watch(mockMasterProxy)
      getActorSystem.stop(mockMasterProxy)
      watcher.expectTerminated(mockMasterProxy)
      // Make sure the parent of mockMasterProxy has received the Terminated message.
      // Issus address: https://github.com/gearpump/gearpump/issues/1919
      Thread.sleep(2000)

      import scala.concurrent.duration._
      mockMasterProxy = getActorSystem.actorOf(Props(new MasterProxy(List(mockMaster.ref.path), 30 seconds)), AppMasterSpec.MOCK_MASTER_PROXY)
      mockMaster.expectMsgClass(15 seconds, classOf[RegisterAppMaster])
    }

    /*

     TODO: This test is failing on Travis randomly
     We have not identifed the root cause.
     Check: https://travis-ci.org/intel-hadoop/gearpump/builds/56826843
     Issue tracker: https://github.com/intel-hadoop/gearpump/issues/733

    "launch executor and task properly" in {
      mockMaster.reply(ResourceAllocated(Array(ResourceAllocation(Resource(4), mockWorker.ref, workerId))))
      mockWorker.expectMsgClass(classOf[LaunchExecutor])

      val workerSystem = ActorSystem("worker", TestUtil.DEFAULT_CONFIG)
      mockWorker.reply(RegisterActorSystem(ActorUtil.getSystemAddress(workerSystem).toString))
      for (i <- 1 to 4) {
        mockMaster.expectMsg(10 seconds, AppMasterSpec.TaskStarted)
      }

      //clock status: task(0,0) -> 1, task(0,1)->0, task(1, 0)->0, task(1,1)->0
      appMaster.tell(UpdateClock(TaskId(0, 0), 1), mockTask.ref)

      //there is no further upstream, so the upstreamMinClock is Long.MaxValue
      mockTask.expectMsg(UpstreamMinClock(Long.MaxValue))

      // check min clock
      appMaster.tell(GetLatestMinClock, mockTask.ref)
      mockTask.expectMsg(LatestMinClock(0))


      //clock status: task(0,0) -> 1, task(0,1)->1, task(1, 0)->0, task(1,1)->0
      appMaster.tell(UpdateClock(TaskId(0, 1), 1), mockTask.ref)

      //there is no further upstream, so the upstreamMinClock is Long.MaxValue
      mockTask.expectMsg(UpstreamMinClock(Long.MaxValue))

      // check min clock
      appMaster.tell(GetLatestMinClock, mockTask.ref)
      mockTask.expectMsg(LatestMinClock(0))

      //clock status: task(0,0) -> 1, task(0,1)->1, task(1, 1)->0, task(1,1)->0
      appMaster.tell(UpdateClock(TaskId(1, 0), 1), mockTask.ref)

      // min clock of processor 0 (Task(0, 0) and Task(0, 1))
      mockTask.expectMsg(UpstreamMinClock(1))

      // check min clock
      appMaster.tell(GetLatestMinClock, mockTask.ref)
      mockTask.expectMsg(LatestMinClock(0))

      //clock status: task(0,0) -> 1, task(0,1)->1, task(1, 1)->0, task(1,1)->1
      appMaster.tell(UpdateClock(TaskId(1, 1), 1), mockTask.ref)

      // min clock of processor 0 (Task(0, 0) and Task(0, 1))
      mockTask.expectMsg(UpstreamMinClock(1))

      // check min clock
      appMaster.tell(GetLatestMinClock, mockTask.ref)
      mockTask.expectMsg(LatestMinClock(1))

      //shutdown worker and all executor on this work, expect appmaster to ask for new resources
      workerSystem.shutdown()
      mockMaster.expectMsg(RequestResource(appId, ResourceRequest(Resource(4), relaxation = Relaxation.ONEWORKER)))
    }
**/

  }

  def ignoreSaveAppData: PartialFunction[Any, Boolean] = {
    case msg: SaveAppData => true
  }
}

object AppMasterSpec {
  val MASTER = "master"
  case object TaskStarted

  val MOCK_MASTER_PROXY = "mockMasterProxy"
}

class TaskA(taskContext : TaskContext, userConf : UserConfig) extends Task(taskContext, userConf) {

  val master = userConf.getValue[ActorRef](AppMasterSpec.MASTER).get
  override def onStart(startTime: StartTime): Unit = {
    master ! AppMasterSpec.TaskStarted
  }

  override def onNext(msg: Message): Unit = {}
}

class TaskB(taskContext : TaskContext, userConf : UserConfig) extends Task(taskContext, userConf) {

  val master = userConf.getValue[ActorRef](AppMasterSpec.MASTER).get
  override def onStart(startTime: StartTime): Unit = {
    master ! AppMasterSpec.TaskStarted
  }

  override def onNext(msg: Message): Unit = {}
}