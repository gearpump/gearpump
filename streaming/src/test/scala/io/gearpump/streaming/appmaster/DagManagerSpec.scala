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

import org.apache.pekko.actor.{ActorSystem, Props, Status}
import org.apache.pekko.testkit.TestProbe
import io.gearpump.cluster.{TestUtil, UserConfig}
import io.gearpump.security._
import io.gearpump.streaming._
import io.gearpump.streaming.appmaster.DagManager.{DAGOperationFailed, DAGOperationSuccess, GetLatestDAG, GetTaskLaunchData, LatestDAG, NewDAGDeployed, ReplaceProcessor, TaskLaunchData, WatchChange}
import io.gearpump.streaming.partitioner.{HashPartitioner, Partitioner}
import io.gearpump.streaming.task.{Subscriber, TaskActor}
import io.gearpump.util.Graph
import io.gearpump.util.Graph._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class DagManagerSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  val hash = Partitioner[HashPartitioner]
  val task1 = ProcessorDescription(id = 1, taskClass = classOf[TaskActor].getName, parallelism = 1)
  val task2 = ProcessorDescription(id = 2, taskClass = classOf[TaskActor].getName, parallelism = 1)
  val graph = Graph(task1 ~ hash ~> task2)
  val dag = DAG(graph)
  implicit var system: ActorSystem = null
  val appId = 0
  val capability = ControlCapability.random()
  def secured(message: Any): ControlRequest = ControlRequest(capability, Some(appId), message)

  lazy val userConfig = UserConfig.empty.withValue(StreamApplication.DAG, graph)

  "Streaming AppMaster" should {
    "deny raw and cross-application replacements at its public entry point" in {
      val master = TestProbe()
      val client = TestProbe()
      val app = io.gearpump.cluster.AppDescription("auth", classOf[AppMaster].getName,
        userConfig, clusterConfig = system.settings.config)
      val appContext = io.gearpump.cluster.AppMasterContext(appId, "owner",
        io.gearpump.cluster.scheduler.Resource(1), null, None, master.ref)
      val actor = system.actorOf(Props(new AppMaster(appContext, app)))
      val mutation = ReplaceProcessor(task2.id, task2.copy(id = 3), false)
      Seq[Any](mutation, ControlRequest(ControlCapability.random(), Some(appId), mutation),
        ControlRequest(capability, Some(appId + 1), mutation)).foreach { denied =>
        client.send(actor, denied)
        client.expectMsgType[Status.Failure]
      }
      system.stop(actor)
    }
  }

  "DagManager" should {
    import io.gearpump.streaming.appmaster.ClockServiceSpec.Store
    "maintain the dags properly" in {
      val store = new Store

      val dagManager = system.actorOf(Props(new DagManager(appId, userConfig, store, Some(dag))))
      val client = TestProbe()
      client.send(dagManager, GetLatestDAG)
      client.expectMsg(LatestDAG(dag))

      client.send(dagManager, GetTaskLaunchData(dag.version, task1.id, null))
      val task1LaunchData = TaskLaunchData(task1, Subscriber.of(task1.id, dag))
      client.expectMsg(task1LaunchData)

      val task2LaunchData = TaskLaunchData(task2, Subscriber.of(task2.id, dag))
      client.send(dagManager, GetTaskLaunchData(dag.version, task2.id, null))
      client.expectMsg(task2LaunchData)

      val watcher = TestProbe()
      client.send(dagManager, WatchChange(watcher.ref))
      val task3 = task2.copy(id = 3, life = LifeTime(100, Long.MaxValue))

      val mutation = ReplaceProcessor(task2.id, task3, inheritConf = false)
      Seq[Any](mutation, ControlRequest(ControlCapability.random(), Some(appId), mutation),
        ControlRequest(capability, Some(appId + 1), mutation)).foreach { unauthorized =>
        client.send(dagManager, unauthorized)
        client.expectMsgType[Status.Failure]
      }
      client.send(dagManager, GetLatestDAG)
      client.expectMsg(LatestDAG(dag))
      client.send(dagManager, secured(mutation))
      client.expectMsg(DAGOperationSuccess)

      client.send(dagManager, GetLatestDAG)
      val newDag = client.expectMsgPF() {
        case LatestDAG(latestDag) => latestDag
      }
      assert(newDag.processors.contains(task3.id))
      watcher.expectMsgType[LatestDAG]

      val task4 = task3.copy(id = 4)
      client.send(dagManager, secured(ReplaceProcessor(task3.id, task4, inheritConf = false)))
      client.expectMsgType[DAGOperationFailed]

      client.send(dagManager, NewDAGDeployed(newDag.version))
      client.send(dagManager, secured(ReplaceProcessor(task3.id, task4, inheritConf = false)))
      client.expectMsg(DAGOperationSuccess)
    }

    "retrieve last stored dag properly" in {
      val store = new Store
      val newGraph = Graph(task1 ~ hash ~> task2)
      val newDag = DAG(newGraph)
      val dagManager = system.actorOf(Props(new DagManager(appId, userConfig, store, Some(newDag))))
      dagManager ! NewDAGDeployed(0)
      val client = TestProbe()
      client.send(dagManager, GetLatestDAG)
      client.expectMsgType[LatestDAG].dag shouldBe newDag
    }
  }

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }

  override def beforeAll(): Unit = {
    this.system = ActorSystem("DagManagerSpec", ControlCapability.runtimeConfig(TestUtil.DEFAULT_CONFIG, appId, capability))
  }
}
