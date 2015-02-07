/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.gearpump.streaming.appmaster

import org.apache.gearpump.streaming.task.{TaskLocations, TaskId}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

class TaskRegistration(appId: Int, totalTaskCount: Int) {

  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)

  private var taskLocations = Map.empty[HostPort, Set[TaskId]]
  private var startedTasks = Set.empty[TaskId]

  def registerTask(taskId: TaskId, host: HostPort): Unit = {
    LOG.info(s"Task $taskId has been Launched for app $appId")

    var taskIds = taskLocations.getOrElse(host, Set.empty[TaskId])
    taskIds += taskId
    taskLocations += host -> taskIds

    startedTasks += taskId
    LOG.info(s" started task size: ${startedTasks.size}")
  }

  def getTaskLocations: TaskLocations = {
    TaskLocations(taskLocations)
  }

  def isAllTasksRegistered: Boolean = {
    totalTaskCount == startedTasks.size
  }
}
