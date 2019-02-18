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

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import io.gearpump.cluster.{TestUtil, UserConfig}
import io.gearpump.streaming.{DAG, LifeTime, ProcessorDescription}
import io.gearpump.streaming.appmaster.ClockService.{ChangeToNewDAG, ChangeToNewDAGSuccess, HealthChecker, ProcessorClock}
import io.gearpump.streaming.appmaster.ClockServiceSpec.Store
import io.gearpump.streaming.partitioner.{HashPartitioner, Partitioner, PartitionerDescription}
import io.gearpump.streaming.storage.AppDataStore
import io.gearpump.streaming.task.{GetLatestMinClock, GetStartClock, UpstreamMinClock, _}
import io.gearpump.util.Graph
import io.gearpump.util.Graph._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import scala.concurrent.{Future, Promise}

class ClockServiceSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ClockServiceSpec", TestUtil.DEFAULT_CONFIG))

  val hash = Partitioner[HashPartitioner]
  val task1 = ProcessorDescription(id = 0, taskClass = classOf[TaskActor].getName, parallelism = 1)
  val task2 = ProcessorDescription(id = 1, taskClass = classOf[TaskActor].getName, parallelism = 1)
  val dag = DAG(Graph(task1 ~ hash ~> task2))
  private val appMaster = TestProbe().ref

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "The ClockService" should {
    "maintain a global view of message timestamp in the application" in {
      val store = new Store()
      val startClock = 100L
      store.put(ClockService.START_CLOCK, startClock)
      val clockService = system.actorOf(Props(new ClockService(dag, appMaster, store)))

      clockService ! GetLatestMinClock
      expectMsg(LatestMinClock(startClock))

      // task(0,0): clock(101); task(1,0): clock(100)
      clockService ! UpdateClock(TaskId(0, 0), 101)
      expectMsg(UpstreamMinClock(None))

      // Min clock is updated
      clockService ! GetLatestMinClock
      expectMsg(LatestMinClock(100))

      // task(0,0): clock(101); task(1,0): clock(101)
      clockService ! UpdateClock(TaskId(1, 0), 101)

      // Upstream is Task(0, 0), 101
      expectMsg(UpstreamMinClock(Some(101)))

      // Min clock is updated
      clockService ! GetLatestMinClock
      expectMsg(LatestMinClock(101))
    }

    "act on ChangeToNewDAG and make sure downstream clock smaller than upstreams" in {
      val store = new Store()
      val startClock = 100L
      store.put(ClockService.START_CLOCK, startClock)
      val clockService = system.actorOf(Props(new ClockService(dag, appMaster, store)))
      val task = TestProbe()
      clockService.tell(UpdateClock(TaskId(0, 0), 200), task.ref)

      val task3 = ProcessorDescription(id = 3, taskClass = classOf[TaskActor].getName,
        parallelism = 1)
      val task4 = ProcessorDescription(id = 4, taskClass = classOf[TaskActor].getName,
        parallelism = 1)
      val task5 = ProcessorDescription(id = 5, taskClass = classOf[TaskActor].getName,
        parallelism = 1)
      val dagAddMiddleNode = DAG(Graph(
        task1 ~ hash ~> task2,
        task1 ~ hash ~> task3,
        task3 ~ hash ~> task2,
        task2 ~ hash ~> task4,
        task5 ~ hash ~> task1
      ), version = 1)
      val user = TestProbe()
      clockService.tell(ChangeToNewDAG(dagAddMiddleNode), user.ref)

      val clocks = user.expectMsgPF() {
        case ChangeToNewDAGSuccess(newDagClocks) =>
          newDagClocks
      }

      // For intermediate task, pick its upstream as initial clock
      assert(clocks(task3.id) == clocks(task1.id))

      // For sink task, pick its upstream as initial clock
      assert(clocks(task4.id) == clocks(task2.id))

      // For source task, set the initial clock as startClock
      assert(clocks(task5.id) == startClock)
    }

    "maintain global checkpoint time" in {
      val store = new Store()
      val startClock = 100L
      store.put(ClockService.START_CLOCK, startClock)
      val clockService = system.actorOf(Props(new ClockService(dag, appMaster, store)))
      clockService ! UpdateClock(TaskId(0, 0), 200L)
      clockService ! UpdateClock(TaskId(1, 0), 200L)
      expectMsgType[UpstreamMinClock]
      expectMsgType[UpstreamMinClock]

      clockService ! GetStartClock
      expectMsg(StartClock(200L))

      val conf = UserConfig.empty.withBoolean("state.checkpoint.enable", value = true)
      val task3 = ProcessorDescription(id = 3, taskClass = classOf[TaskActor].getName,
        parallelism = 1, taskConf = conf)
      val task4 = ProcessorDescription(id = 4, taskClass = classOf[TaskActor].getName,
        parallelism = 1, taskConf = conf)
      val dagWithStateTasks = DAG(Graph(
        task1 ~ hash ~> task2,
        task1 ~ hash ~> task3,
        task3 ~ hash ~> task2,
        task2 ~ hash ~> task4
      ), version = 1)

      val taskId3 = TaskId(3, 0)
      val taskId4 = TaskId(4, 0)

      clockService ! ChangeToNewDAG(dagWithStateTasks)
      expectMsgType[ChangeToNewDAGSuccess]

      clockService ! UpdateCheckpointClock(taskId3, startClock)
      clockService ! UpdateCheckpointClock(taskId4, startClock)
      clockService ! GetStartClock
      expectMsg(StartClock(startClock))

      clockService ! UpdateCheckpointClock(taskId3, 200L)
      clockService ! UpdateCheckpointClock(taskId4, 300L)
      clockService ! GetStartClock
      expectMsg(StartClock(startClock))

      clockService ! UpdateCheckpointClock(taskId3, 300L)
      clockService ! GetStartClock
      expectMsg(StartClock(300L))
    }
  }

  "ProcessorClock" should {
    "maintain the min clock of current processor" in {
      val processorId = 0
      val parallism = 3
      val clock = new ProcessorClock(processorId, LifeTime.Immortal, parallism)
      clock.init(100L)
      clock.updateMinClock(0, 101)
      assert(clock.min == 100L)

      clock.updateMinClock(1, 102)
      assert(clock.min == 100L)

      clock.updateMinClock(2, 103)
      assert(clock.min == 101L)
    }
  }

  "HealthChecker" should {
    "report stalling if the clock is not advancing" in {
      val healthChecker = new HealthChecker(stallingThresholdSeconds = 1)
      val source = ProcessorDescription(id = 0, taskClass = null, parallelism = 1)
      val sourceClock = new ProcessorClock(0, LifeTime.Immortal, 1)
      sourceClock.init(0L)
      val sink = ProcessorDescription(id = 1, taskClass = null, parallelism = 1)
      val sinkClock = new ProcessorClock(1, LifeTime.Immortal, 1)
      sinkClock.init(0L)
      val graph = Graph.empty[ProcessorDescription, PartitionerDescription]
      graph.addVertex(source)
      graph.addVertex(sink)
      graph.addVertexAndEdge(source, PartitionerDescription(null), sink)
      val dag = DAG(graph)
      val clocks = Map(
        0 -> sourceClock,
        1 -> sinkClock
      )

      sourceClock.updateMinClock(0, 100L)
      sinkClock.updateMinClock(0, 100L)

      // Clock advances from 0 to 100, there is no stalling.
      healthChecker.check(currentMinClock = 100, clocks, dag, 200)
      healthChecker.getReport.stallingTasks shouldBe List.empty[TaskId]

      // Clock not advancing.
      // Pasted time exceed the stalling threshold, report stalling
      healthChecker.check(currentMinClock = 100, clocks, dag, 1300)

      // The source task is stalling the clock
      healthChecker.getReport.stallingTasks shouldBe List(TaskId(0, 0))

      // Advance the source clock
      sourceClock.updateMinClock(0, 101L)
      healthChecker.check(currentMinClock = 100, clocks, dag, 1300)
      // The sink task is stalling the clock
      healthChecker.getReport.stallingTasks shouldBe List(TaskId(1, 0))
    }
  }
}

object ClockServiceSpec {

  class Store extends AppDataStore {

    private var map = Map.empty[String, Any]

    def put(key: String, value: Any): Future[Any] = {
      map = map + (key -> value)
      Promise.successful(value).future
    }

    def get(key: String): Future[Any] = {
      Promise.successful(map.getOrElse(key, null)).future
    }
  }
}
