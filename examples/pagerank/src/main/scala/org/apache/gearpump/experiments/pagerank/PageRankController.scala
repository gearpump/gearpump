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
package org.apache.gearpump.experiments.pagerank

import java.time.Instant

import akka.actor.Actor.Receive

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.pagerank.PageRankController.Tick
import org.apache.gearpump.experiments.pagerank.PageRankWorker.LatestWeight
import org.apache.gearpump.streaming.task._

class PageRankController(taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {

  val taskCount = conf.getInt(PageRankApplication.COUNT).get
  val iterationMax = conf.getInt(PageRankApplication.ITERATION).get
  val delta = conf.getDouble(PageRankApplication.DELTA).get

  val tasks = (0 until taskCount).toList.map(TaskId(1, _))

  var tick: Int = 0
  var receivedWeightForCurrentTick = 0

  var weights = Map.empty[TaskId, Double]
  var deltas = Map.empty[TaskId, Double]

  override def onStart(startTime: Instant): Unit = {
    output(Tick(tick), tasks: _*)
  }

  private def output(msg: AnyRef, tasks: TaskId*): Unit = {
    taskContext.asInstanceOf[TaskWrapper].outputUnManaged(msg, tasks: _*)
  }

  override def receiveUnManagedMessage: Receive = {
    case LatestWeight(taskId, weight, replyTick) =>
      if (this.tick == replyTick) {

        deltas += taskId -> Math.abs(weight - weights.getOrElse(taskId, 0.0))
        weights += taskId -> weight
        receivedWeightForCurrentTick += 1
        if (receivedWeightForCurrentTick == taskCount) {
          this.tick += 1
          receivedWeightForCurrentTick = 0
          if (continueIteration) {
            LOG.debug(s"next iteration: $tick, weight: $weights, delta: $deltas")
            output(Tick(tick), tasks: _*)
          } else {
            LOG.info(s"iterations: $tick, weight: $weights, delta: $deltas")
          }
        }
      }
  }

  private def continueIteration: Boolean = {
    (tick < iterationMax) && deltas.values.foldLeft(false) { (deltaExceed, value) =>
      deltaExceed || value > delta
    }
  }
}

object PageRankController {
  case class Tick(iteration: Int)
}
