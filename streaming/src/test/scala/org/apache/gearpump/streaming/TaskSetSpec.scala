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

import org.apache.gearpump.cluster.scheduler.{Relaxation, Resource, ResourceRequest}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.util.Graph
import org.scalatest.{Matchers, WordSpec}

import org.apache.gearpump.util.Graph._

import scala.collection.mutable.ArrayBuffer
import org.apache.gearpump.util.Constants._

class TaskSetSpec extends WordSpec with Matchers {
  val task1 = TaskDescription(classOf[TestTask1].getName, 4)
  val task2 = TaskDescription(classOf[TestTask2].getName, 2)
  val partitioner = new HashPartitioner()
  val dag = DAG(Graph(task1 ~ partitioner ~> task2))
  val resource = getClass.getClassLoader.getResource("tasklocation.conf").getPath
  System.setProperty(GEARPUMP_CUSTOM_CONFIG_FILE, resource)

  "TaskSet" should {
    "schedule tasks on different workers properly according user's configuration" in {
      val taskSet = new TaskSet(0, dag)
      assert(taskSet.hasNotLaunchedTask)
      assert(taskSet.size == 6)

      val resoureRequest1 = ResourceRequest(Resource(4), 1, relaxation = Relaxation.SPECIFICWORKER)
      val resoureRequest2 = ResourceRequest(Resource(2), 2, relaxation = Relaxation.SPECIFICWORKER)
      val expectedRequests = Array(resoureRequest1, resoureRequest2)
      val acturalRequests = taskSet.fetchResourceRequests().sortBy(_.resource.slots)
      assert(acturalRequests.sameElements(expectedRequests.sortBy(_.resource.slots)))

      val tasksOnWorker1 = ArrayBuffer[Int]()
      val tasksOnWorker2 = ArrayBuffer[Int]()
      for (i <- 0 until 4) {
        tasksOnWorker1.append(taskSet.scheduleTaskOnWorker(1).taskId.groupId)
      }
      for (i <- 0 until 2) {
        tasksOnWorker2.append(taskSet.scheduleTaskOnWorker(2).taskId.groupId)
      }

      assert(taskSet.scheduleTaskOnWorker(3) == null)

      assert(tasksOnWorker1.sorted.sameElements(Array(0, 0, 1, 1)))
      assert(tasksOnWorker2.sorted.sameElements(Array(0, 0)))

      assert(!taskSet.hasNotLaunchedTask)
      assert(taskSet.size == 0)

      val failedTasks = Array(TaskId(0, 0), TaskId(0, 1))
      taskSet.taskFailed(failedTasks)
      assert(taskSet.fetchResourceRequests().sameElements(Array(ResourceRequest(Resource(2)))))
      assert(taskSet.fetchResourceRequests(true).sameElements(Array(ResourceRequest(Resource(2), relaxation = Relaxation.ONEWORKER))))

      assert(taskSet.scheduleTaskOnWorker(3).taskId.groupId == 0)
    }
  }
}
