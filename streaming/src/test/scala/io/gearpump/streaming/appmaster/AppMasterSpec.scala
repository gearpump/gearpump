/*
 * Licensed under the Apache License, Version 2.0 (the
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
package io.gearpump.streaming.appmaster

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestProbe}
import io.gearpump.cluster.{MasterHarness, TestUtil, UserConfig, _}
import io.gearpump.cluster.AppMasterToMaster._
import io.gearpump.cluster.AppMasterToWorker.LaunchExecutor
import io.gearpump.cluster.ClientToMaster.GetLastFailure
import io.gearpump.cluster.MasterToAppMaster._
import io.gearpump.cluster.MasterToClient.LastFailure
import io.gearpump.cluster.WorkerToAppMaster.ExecutorLaunchRejected
import io.gearpump.cluster.appmaster.{ApplicationRuntimeInfo, AppMasterRuntimeEnvironment}
import io.gearpump.cluster.master.MasterProxy
import io.gearpump.cluster.scheduler.{Resource, ResourceAllocation, ResourceRequest}
import io.gearpump.cluster.worker.WorkerId
import io.gearpump.jarstore.FilePath
import io.gearpump.streaming.{DAG, Processor, StreamApplication}
import io.gearpump.streaming.AppMasterToExecutor.StopTask
import io.gearpump.streaming.ExecutorToAppMaster.{MessageLoss, UnRegisterTask}
import io.gearpump.streaming.appmaster.AppMaster.{LookupTaskActorRef, TaskActorRef}
import io.gearpump.streaming.partitioner.HashPartitioner
import io.gearpump.streaming.task.{TaskContext, _}
import io.gearpump.util.{ActorUtil, Graph}
import io.gearpump.util.ActorSystemBooter.RegisterActorSystem
import io.gearpump.util.Graph._
import org.scalatest._
import scala.concurrent.duration._

class AppMasterSpec extends WordSpec with Matchers with BeforeAndAfterEach with MasterHarness {
  protected override def config = TestUtil.DEFAULT_CONFIG

  var appMaster: ActorRef = null

  val appId = 0
  val invalidAppId = -1
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
  var appMasterRuntimeInfo: ApplicationRuntimeInfo = null

  override def beforeEach(): Unit = {
    startActorSystem()

    mockTask = TestProbe()(getActorSystem)

    mockMaster = TestProbe()(getActorSystem)
    mockWorker = TestProbe()(getActorSystem)
    mockMaster.ignoreMsg(ignoreSaveAppData)
    appMasterRuntimeInfo = ApplicationRuntimeInfo(appId, appName = appId.toString)

    implicit val system = getActorSystem
    conf = UserConfig.empty.withValue(AppMasterSpec.MASTER, mockMaster.ref)
    val mockJar = Some(AppJar("for_test", FilePath("path")))
    appMasterContext = AppMasterContext(appId, "test", resource, null, mockJar, mockMaster.ref)
    val graph = Graph(taskDescription1 ~ partitioner ~> taskDescription2)
    val streamApp = StreamApplication("test", graph, conf)
    appDescription = Application.ApplicationToAppDescription(streamApp)
    import scala.concurrent.duration._
    mockMasterProxy = getActorSystem.actorOf(Props(new MasterProxy(List(mockMaster.ref.path),
      30.seconds)), AppMasterSpec.MOCK_MASTER_PROXY)
    TestActorRef[AppMaster](
      AppMasterRuntimeEnvironment.props(List(mockMasterProxy.path), appDescription,
        appMasterContext))(getActorSystem)

    val registerAppMaster = mockMaster.receiveOne(15.seconds)
    registerAppMaster shouldBe a [RegisterAppMaster]
    appMaster = registerAppMaster.asInstanceOf[RegisterAppMaster].appMaster

    mockMaster.reply(AppMasterRegistered(appId))
    mockMaster.expectMsg(15.seconds, GetAppData(appId, "DAG"))
    mockMaster.reply(GetAppDataResult("DAG", null))
    mockMaster.expectMsg(15.seconds, GetAppData(appId, "startClock"))

    mockMaster.reply(GetAppDataResult("startClock", 0L))

    mockMaster.expectMsg(15.seconds, RequestResource(appId, ResourceRequest(Resource(4),
      workerId = WorkerId.unspecified)))
  }

  override def afterEach(): Unit = {
    shutdownActorSystem()
  }

  "AppMaster" should {
    "kill itself when allocate resource time out" in {
      // not enough resource allocated
      // triggers ResourceAllocationTimeout in ExecutorSystemScheduler
      mockMaster.reply(ResourceAllocated(Array(ResourceAllocation(Resource(2),
        mockWorker.ref, workerId))))
      val statusChanged = mockMaster.expectMsgType[ApplicationStatusChanged](30.seconds)
      statusChanged.newStatus.isInstanceOf[ApplicationStatus.FAILED] shouldBe true
    }

    "reschedule the resource when the worker reject to start executor" in {
      val resource = Resource(4)
      mockMaster.reply(ResourceAllocated(Array(ResourceAllocation(resource,
        mockWorker.ref, workerId))))
      mockWorker.expectMsgClass(classOf[LaunchExecutor])
      mockWorker.reply(ExecutorLaunchRejected(""))
      mockMaster.expectMsg(RequestResource(appId, ResourceRequest(resource, WorkerId.unspecified)))
    }

    "find a new master when lost connection with master" in {

      val watcher = TestProbe()(getActorSystem)
      watcher.watch(mockMasterProxy)
      getActorSystem.stop(mockMasterProxy)
      watcher.expectTerminated(mockMasterProxy)
      // Make sure the parent of mockMasterProxy has received the Terminated message.
      // Issues address: https://github.com/gearpump/gearpump/issues/1919
      Thread.sleep(2000)

      import scala.concurrent.duration._
      mockMasterProxy = getActorSystem.actorOf(Props(new MasterProxy(List(mockMaster.ref.path),
        30.seconds)), AppMasterSpec.MOCK_MASTER_PROXY)
      mockMaster.expectMsgClass(15.seconds, classOf[RegisterAppMaster])
    }

    "launch executor and task properly" in {
      val workerSystem = startApp()
      expectAppStarted()

      appMaster.tell(UpdateClock(TaskId(0, 0), 0), mockTask.ref)
      mockTask.expectMsg(UpstreamMinClock(None))
      appMaster.tell(UpdateClock(TaskId(0, 1), 0), mockTask.ref)
      mockTask.expectMsg(UpstreamMinClock(None))
      appMaster.tell(UpdateClock(TaskId(1, 0), 0), mockTask.ref)
      mockTask.expectMsg(UpstreamMinClock(Some(0)))
      appMaster.tell(UpdateClock(TaskId(1, 1), 0), mockTask.ref)
      mockTask.expectMsg(UpstreamMinClock(Some(0)))

      // clock status: task(0,0) -> 1, task(0,1)->0, task(1,0)->0, task(1,1)->0
      appMaster.tell(UpdateClock(TaskId(0, 0), 1), mockTask.ref)
      mockTask.expectMsg(UpstreamMinClock(None))

      // check min clock
      appMaster.tell(GetLatestMinClock, mockTask.ref)
      mockTask.expectMsg(LatestMinClock(0))

      // clock status: task(0,0) -> 1, task(0,1)->1, task(1, 0)->0, task(1,1)->0
      appMaster.tell(UpdateClock(TaskId(0, 1), 1), mockTask.ref)
      mockTask.expectMsg(UpstreamMinClock(None))

      // check min clock
      appMaster.tell(GetLatestMinClock, mockTask.ref)
      mockTask.expectMsg(LatestMinClock(0))

      // Clock status: task(0,0) -> 1, task(0,1)->1, task(1, 1)->0, task(1,1)->0
      appMaster.tell(UpdateClock(TaskId(1, 0), 1), mockTask.ref)

      // Min clock of processor 0 (Task(0, 0) and Task(0, 1))
      mockTask.expectMsg(UpstreamMinClock(Some(1)))

      // check min clock
      appMaster.tell(GetLatestMinClock, mockTask.ref)
      mockTask.expectMsg(LatestMinClock(0))

      // clock status: task(0,0) -> 1, task(0,1)->1, task(1, 1)->0, task(1,1)->1
      appMaster.tell(UpdateClock(TaskId(1, 1), 1), mockTask.ref)

      // min clock of processor 0 (Task(0, 0) and Task(0, 1))
      mockTask.expectMsg(UpstreamMinClock(Some(1)))

      // check min clock
      appMaster.tell(GetLatestMinClock, mockTask.ref)
      mockTask.expectMsg(LatestMinClock(1))

      // unregister task
      for (i <- 0 to 1) {
        appMaster.tell(UnRegisterTask(TaskId(i, 1), 0), mockTask.ref)
        mockTask.expectMsg(StopTask(TaskId(i, 1)))
      }

      workerSystem.terminate()
    }

    "serve AppMaster data request" in {
      val workerSystem = startApp()
      expectAppStarted()

      // get DAG
      appMaster.tell(GetDAG, mockTask.ref)
      mockTask.expectMsgType[DAG]

      // get appmaster data
      appMaster.tell(AppMasterDataDetailRequest(appId), mockTask.ref)
      mockTask.expectMsgType[StreamAppMasterSummary](30.seconds)
      appMaster.tell(AppMasterDataDetailRequest(invalidAppId), mockTask.ref)
      mockTask.expectNoMessage()

      for {
        i <- 0 to 1
        j <- 0 to 1
      } {
        // lookup task ActorRef
        appMaster.tell(LookupTaskActorRef(TaskId(i, j)), mockTask.ref)
        mockTask.expectMsgType[TaskActorRef]
      }

      workerSystem.terminate()
    }

    "replay on message loss" in {
      val workerSystem = startApp()
      expectAppStarted()

      for (i <- 1 to 5) {
        val taskId = TaskId(0, 0)
        appMaster.tell(UpdateClock(taskId, i), mockTask.ref)
        mockTask.expectMsgType[UpstreamMinClock]

        val cause = s"message loss $i from $taskId"
        appMaster.tell(MessageLoss(0, taskId, cause), mockTask.ref)
        // appmaster restarted
        expectAppStarted()

        appMaster.tell(GetLastFailure(appId), mockTask.ref)
        val failure = mockTask.expectMsgType[LastFailure]
        failure.error shouldBe cause

        appMaster.tell(GetLastFailure(invalidAppId), mockTask.ref)
        mockTask.expectNoMessage()
      }

      // fail to recover after restarting a tasks for 5 times
      appMaster.tell(MessageLoss(0, TaskId(0, 0), "message loss"), mockTask.ref)
      val statusChanged = mockMaster.expectMsgType[ApplicationStatusChanged](60.seconds)
      statusChanged.newStatus.isInstanceOf[ApplicationStatus.FAILED] shouldBe true

      workerSystem.terminate()
    }

    "replay on client request" in {
      startApp()
      expectAppStarted()

      appMaster.tell(ReplayFromTimestampWindowTrailingEdge(appId), mockTask.ref)
      expectAppStarted()

      appMaster.tell(ReplayFromTimestampWindowTrailingEdge(invalidAppId), mockTask.ref)
      mockMaster.expectNoMessage()
    }
  }

  def ignoreSaveAppData: PartialFunction[Any, Boolean] = {
    case _: SaveAppData => true
  }

  def startApp(): ActorSystem = {
    mockMaster.reply(ResourceAllocated(Array(ResourceAllocation(Resource(4), mockWorker.ref,
      workerId))))
    mockWorker.expectMsgClass(classOf[LaunchExecutor])

    val workerSystem = ActorSystem("worker", TestUtil.DEFAULT_CONFIG)
    mockWorker.reply(RegisterActorSystem(ActorUtil.getSystemAddress(workerSystem).toString))
    workerSystem
  }

  def expectAppStarted(): Unit = {
    // wait for app to get started
    mockMaster.expectMsgType[ApplicationStatusChanged]
    mockMaster.reply(AppMasterActivated(appId))
  }
}

object AppMasterSpec {
  val MASTER = "master"
  case object TaskStarted

  val MOCK_MASTER_PROXY = "mockMasterProxy"
}

class TaskA(taskContext: TaskContext, userConf: UserConfig) extends Task(taskContext, userConf) {
}

class TaskB(taskContext: TaskContext, userConf: UserConfig) extends Task(taskContext, userConf) {
}