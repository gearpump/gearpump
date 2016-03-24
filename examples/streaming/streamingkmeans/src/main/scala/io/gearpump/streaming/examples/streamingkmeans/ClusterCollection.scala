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

package io.gearpump.streaming.examples.streamingkmeans

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.task.{StartTime, Task, TaskContext}

class ClusterCollection(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  import taskContext.output

  private val k = conf.getInt("k").get
  private val maxNumber = conf.getInt("maxNumber").get

  private[streamingkmeans] var minTaskId = 0
  private[streamingkmeans] var minDistance = Double.MaxValue
  private[streamingkmeans] var minDistPoint : List[Double] = null

  private[streamingkmeans] var currentNumber = 0
  private[streamingkmeans] var totalNumber = 0

  override def onStart(startTime: StartTime): Unit = super.onStart(startTime)

  override def onNext(msg: Message): Unit = {
    if (null == msg) {
      return
    }

    val (taskId, distance, point) = msg.msg.asInstanceOf[(Int, Double, List[Double])]
    if (distance < minDistance) {
      minDistance = distance
      minDistPoint = point
      minTaskId = taskId
    }

    currentNumber += 1
    if (k == currentNumber) {
      currentNumber = 0
      totalNumber += 1
      if (maxNumber == totalNumber) {
        totalNumber = 0
        output(new Message(new ResultMessage(minTaskId, minDistPoint, true)))
      } else {
        output(new Message(new ResultMessage(minTaskId, minDistPoint, false)))
      }
    }
  }

  override def onStop(): Unit = super.onStop()
}
