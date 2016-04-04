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

import com.typesafe.config.ConfigFactory
import io.gearpump.streaming.Constants
import io.gearpump.streaming.appmaster.TaskLocator.Localities
import io.gearpump.streaming.task.{StartTime, TaskContext, TaskId}
import io.gearpump.{WorkerId, Message}
import io.gearpump.cluster.scheduler.{Relaxation, Resource, ResourceRequest}
import io.gearpump.cluster.{TestUtil, ClusterConfig, UserConfig}
import io.gearpump.partitioner.{HashPartitioner, Partitioner}
import io.gearpump.streaming.appmaster.TaskLocator.Localities
import io.gearpump.streaming.appmaster.TaskSchedulerSpec.{TestTask1, TestTask2}
import io.gearpump.streaming.task.{StartTime, Task, TaskContext, TaskId}
import io.gearpump.streaming.{DAG, ProcessorDescription}
import io.gearpump.util.Graph
import io.gearpump.util.Graph._
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable.ArrayBuffer

class TaskSchedulerSpec extends WordSpec with Matchers {
  val task1 = ProcessorDescription(id = 0, taskClass = classOf[TestTask1].getName, parallelism = 4)
  val task2 = ProcessorDescription(id = 1, taskClass = classOf[TestTask2].getName, parallelism = 2)

  val dag = DAG(Graph(task1 ~ Partitioner[HashPartitioner] ~> task2))

  val config = TestUtil.DEFAULT_CONFIG

  "TaskScheduler" should {
    "schedule tasks on different workers properly according user's configuration" in {

      val localities = Localities(
        Map(WorkerId(1, 0L) -> Array(TaskId(0,0), TaskId(0,1), TaskId(1,0), TaskId(1,1)),
          WorkerId(2, 0L) -> Array(TaskId(0,2), TaskId(0,3))
      ))

      val localityConfig = ConfigFactory.parseString(Localities.toJson(localities))

      import Constants.GEARPUMP_STREAMING_LOCALITIES
      val appName = "app"
      val taskScheduler = new TaskSchedulerImpl(appId = 0, appName,
        config.withValue(s"$GEARPUMP_STREAMING_LOCALITIES.$appName", localityConfig.root))

      val expectedRequests =
        Array( ResourceRequest(Resource(4), WorkerId(1, 0L), relaxation = Relaxation.SPECIFICWORKER),
          ResourceRequest(Resource(2), WorkerId(2, 0L), relaxation = Relaxation.SPECIFICWORKER))

      taskScheduler.setDAG(dag)
      val resourceRequests = taskScheduler.getResourceRequests()

      val acturalRequests = resourceRequests.sortBy(_.resource.slots)
      assert(acturalRequests.sameElements(expectedRequests.sortBy(_.resource.slots)))

      val tasksOnWorker1 = ArrayBuffer[Int]()
      val tasksOnWorker2 = ArrayBuffer[Int]()
      for (i <- 0 until 4) {
        tasksOnWorker1.append(taskScheduler.schedule(WorkerId(1, 0L), executorId = 0, Resource(1)).head.processorId)
      }
      for (i <- 0 until 2) {
        tasksOnWorker2.append(taskScheduler.schedule(WorkerId(2, 0L), executorId = 1, Resource(1)).head.processorId)
      }

      //allocate more resource, and no tasks to launch
      assert(taskScheduler.schedule(WorkerId(3, 0L), executorId = 3, Resource(1)) == List.empty[TaskId])

      //on worker1, executor 0
      assert(tasksOnWorker1.sorted.sameElements(Array(0, 0, 1, 1)))

      //on worker2, executor 1, Task(0, 0), Task(0, 1)
      assert(tasksOnWorker2.sorted.sameElements(Array(0, 0)))

      val rescheduledResources = taskScheduler.executorFailed(executorId = 1)

      assert(rescheduledResources.sameElements(Array(ResourceRequest(Resource(2), WorkerId.unspecified, relaxation = Relaxation.ONEWORKER))))

      val launchedTask = taskScheduler.schedule(WorkerId(3, 0L), executorId = 3, Resource(2))

      //start the failed 2 tasks Task(0, 0) and Task(0, 1)
      assert(launchedTask.length == 2)
    }

    "schedule task fairly" in {
      val appName = "app"
      val taskScheduler = new TaskSchedulerImpl(appId = 0, appName, config)

      val expectedRequests =
        Array( ResourceRequest(Resource(4), WorkerId(1, 0L), relaxation = Relaxation.SPECIFICWORKER),
          ResourceRequest(Resource(2), WorkerId(2, 0L), relaxation = Relaxation.SPECIFICWORKER))

      taskScheduler.setDAG(dag)
      val tasks = taskScheduler.schedule(WorkerId(1, 0L), executorId = 0, Resource(4))
      assert(tasks.filter(_.processorId == 0).length == 2)
      assert(tasks.filter(_.processorId == 1).length == 2)
    }
  }
}

object TaskSchedulerSpec{
  class TestTask1(taskContext : TaskContext, userConf : UserConfig)
      extends Task(taskContext, userConf) {
    override def onStart(startTime: StartTime): Unit = ???
    override def onNext(msg: Message): Unit = ???
  }

  class TestTask2(taskContext : TaskContext, userConf : UserConfig)
      extends Task(taskContext, userConf) {
    override def onStart(startTime: StartTime): Unit = ???
    override def onNext(msg: Message): Unit = ???
  }
}
