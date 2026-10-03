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
package io.gearpump.streaming.task

import org.apache.pekko.actor.{ExtendedActorSystem, Props}
import org.apache.pekko.testkit._
import com.typesafe.config.{Config, ConfigFactory}
import io.gearpump.Message
import io.gearpump.cluster.{MasterHarness, TestUtil}
import io.gearpump.serializer.{FastKryoSerializer, SerializationFramework}
import io.gearpump.streaming.{DAG, LifeTime, ProcessorDescription}
import io.gearpump.streaming.AppMasterToExecutor.{ChangeTask, MsgLostException, StartTask, TaskChanged, TaskRegistered}
import io.gearpump.streaming.partitioner.{HashPartitioner, Partitioner}
import io.gearpump.streaming.task.TaskActorSpec.TestTask
import io.gearpump.util.{Graph, Util}
import io.gearpump.util.Graph._
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TaskActorSpec extends AnyWordSpec with Matchers with BeforeAndAfterEach with MasterHarness {
  protected override def config: Config = {
    ConfigFactory.parseString(
      """ pekko.loggers = ["org.apache.pekko.testkit.TestEventListener"]
        | pekko.test.filter-leeway = 20000
      """.stripMargin).
      withFallback(io.gearpump.security.ControlCapability.runtimeConfig(TestUtil.DEFAULT_CONFIG, 0, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"))
  }

  val appId = 0
  val task1 = ProcessorDescription(id = 0, taskClass = classOf[TestTask].getName, parallelism = 1)
  val task2 = ProcessorDescription(id = 1, taskClass = classOf[TestTask].getName, parallelism = 1)
  val dag: DAG = DAG(Graph(task1 ~ Partitioner[HashPartitioner] ~> task2))
  val taskId1 = TaskId(0, 0)
  val taskId2 = TaskId(1, 0)
  val executorId1 = 1
  val executorId2 = 2

  var mockMaster: TestProbe = null
  var taskContext1: TaskContextData = null

  var mockSerializerPool: SerializationFramework = null

  override def beforeEach(): Unit = {
    startActorSystem()
    mockMaster = TestProbe()(getActorSystem)

    mockSerializerPool = mock(classOf[SerializationFramework])
    val serializer = new FastKryoSerializer(getActorSystem.asInstanceOf[ExtendedActorSystem])
    when(mockSerializerPool.get()).thenReturn(serializer)

    taskContext1 = TaskContextData(executorId1, appId,
      "appName", mockMaster.ref, 1,
      LifeTime.Immortal,
      Subscriber.of(processorId = 0, dag))
  }

  "TaskActor" should {
    "deserialize and deliver records after a valid predecessor session enrolls" in {
      val mockTask = mock(classOf[TaskWrapper])
      val testActor = TestActorRef[TaskActor](Props(new TaskActor(taskId1,
        taskContext1.copy(upstream = Map(2 -> 1)), mockTask, mockSerializerPool)))(getActorSystem)
      testActor ! TaskRegistered(taskId1, 0, Util.randInt())
      testActor ! StartTask(taskId1)
      val source = TaskId(2, 0)
      val identity = io.gearpump.util.PekkoHelper.sessionActorFor(getActorSystem, 123,
        TaskId.toLong(source))
      val serializer = new FastKryoSerializer(getActorSystem.asInstanceOf[ExtendedActorSystem])
      val bytes = serializer.serialize("allowed")
      testActor.tell(InitialAckRequest(source, 123), identity)
      testActor.tell(SerializedMessage(0L, bytes), identity)
      verify(mockSerializerPool, times(1)).get()
      verify(mockTask, times(1)).onNext(Message("allowed", 0L))
    }

    "discard serialized records before deserializing an unknown sender" in {
      val mockTask = mock(classOf[TaskWrapper])
      val testActor = TestActorRef[TaskActor](Props(new TaskActor(taskId1, taskContext1,
        mockTask, mockSerializerPool)))(getActorSystem)
      testActor ! TaskRegistered(taskId1, 0, Util.randInt())
      testActor ! StartTask(taskId1)
      mockMaster.send(testActor, SerializedMessage(0L, Array[Byte](1, 2)))
      verify(mockSerializerPool, org.mockito.Mockito.never()).get()
      verify(mockTask, org.mockito.Mockito.never()).onNext(org.mockito.ArgumentMatchers.any[Message]())
    }

    "register itself to AppMaster when started" in {
      val mockTask = mock(classOf[TaskWrapper])
      val testActor = TestActorRef[TaskActor](Props(
        new TaskActor(taskId1, taskContext1,
          mockTask, mockSerializerPool)))(getActorSystem)
      testActor ! TaskRegistered(taskId1, 0, Util.randInt())
      testActor ! StartTask(taskId1)

      implicit val system = getActorSystem
      val ack = Ack(taskId2, 100, 99, testActor.underlyingActor.sessionId, 1024L)
      EventFilter[MsgLostException](occurrences = 1) intercept {
        testActor ! ack
      }
    }

    "respond to ChangeTask" in {
      val mockTask = mock(classOf[TaskWrapper])
      val testActor = TestActorRef[TaskActor](Props(new TaskActor(taskId1, taskContext1,
        mockTask, mockSerializerPool)))(getActorSystem)
      testActor ! TaskRegistered(taskId1, 0, Util.randInt())
      testActor ! StartTask(taskId1)
      mockMaster.expectMsgType[GetUpstreamMinClock]

      mockMaster.send(testActor, ChangeTask(taskId1, 1, LifeTime.Immortal, List.empty[Subscriber]))
      mockMaster.expectMsgType[TaskChanged]
    }

    "handle received message correctly" in {
      val mockTask = mock(classOf[TaskWrapper])
      val msg = Message("test")

      val testActor = TestActorRef[TaskActor](Props(new TaskActor(taskId1, taskContext1,
        mockTask, mockSerializerPool)))(getActorSystem)
      testActor.tell(TaskRegistered(taskId1, 0, Util.randInt()), mockMaster.ref)
      testActor.tell(StartTask(taskId1), mockMaster.ref)

      testActor.tell(msg, testActor)

      verify(mockTask, times(1)).onNext(msg)
    }
  }

  override def afterEach(): Unit = {
    shutdownActorSystem()
  }
}

object TaskActorSpec {
  class TestTask
}
