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
package org.apache.gearpump.streaming.appmaster

import org.apache.gearpump.cluster.ClusterConfig
import org.apache.gearpump.cluster.scheduler.{Relaxation, Resource, ResourceRequest}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.{DAG, ProcessorDescription}
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.Graph._
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable.ArrayBuffer

class TaskSchedulerSpec extends WordSpec with Matchers {
  val task1 = ProcessorDescription(classOf[TestTask1].getName, 4)
  val task2 = ProcessorDescription(classOf[TestTask2].getName, 2)
  val partitioner = new HashPartitioner()
  val dag = DAG(Graph(task1 ~ partitioner ~> task2))
  val resource = getClass.getClassLoader.getResource("tasklocation.conf").getPath
  System.setProperty(GEARPUMP_CUSTOM_CONFIG_FILE, resource)

  val config = ClusterConfig.load.application

  "TaskScheduler" should {
    "schedule tasks on different workers properly according user's configuration" in {
      val taskScheduler = new TaskSchedulerImpl(appId = 0, config)

      val expectedRequests =
        Array( ResourceRequest(Resource(4), 1, relaxation = Relaxation.SPECIFICWORKER),
          ResourceRequest(Resource(2), 2, relaxation = Relaxation.SPECIFICWORKER))

      taskScheduler.setTaskDAG(dag)
      val resourceRequests = taskScheduler.getResourceRequests()

      val acturalRequests = resourceRequests.sortBy(_.resource.slots)
      assert(acturalRequests.sameElements(expectedRequests.sortBy(_.resource.slots)))

      val tasksOnWorker1 = ArrayBuffer[Int]()
      val tasksOnWorker2 = ArrayBuffer[Int]()
      for (i <- 0 until 4) {
        tasksOnWorker1.append(taskScheduler.resourceAllocated(1, executorId = 0).get.taskId.processorId)
      }
      for (i <- 0 until 2) {
        tasksOnWorker2.append(taskScheduler.resourceAllocated(2, executorId = 1).get.taskId.processorId)
      }

      //allocate more resource, and no tasks to launch
      assert(taskScheduler.resourceAllocated(3, executorId = 3) == None)

      //on worker1, executor 0
      assert(tasksOnWorker1.sorted.sameElements(Array(0, 0, 1, 1)))

      //on worker2, executor 1, Task(0, 0), Task(0, 1)
      assert(tasksOnWorker2.sorted.sameElements(Array(0, 0)))

      val rescheduledResources = taskScheduler.executorFailed(executorId = 1)

      assert(rescheduledResources.sameElements(Array(ResourceRequest(Resource(2), relaxation = Relaxation.ONEWORKER))))

      val launchedTask = taskScheduler.resourceAllocated(workerId  = 3, executorId = 3)

      //start the failed 2 tasks Task(0, 0) and Task(0, 1)
      assert(launchedTask.get.taskId.processorId == 0)
    }
  }
}
