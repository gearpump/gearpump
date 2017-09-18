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

import akka.actor.Actor.Receive

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.pagerank.PageRankApplication.NodeWithTaskId
import org.apache.gearpump.experiments.pagerank.PageRankController.Tick
import org.apache.gearpump.experiments.pagerank.PageRankWorker.{LatestWeight, UpdateWeight}
import org.apache.gearpump.streaming.task.{Task, TaskContext, TaskId, TaskWrapper}
import org.apache.gearpump.util.Graph

class PageRankWorker(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  import taskContext.taskId

  private var weight: Double = 1.0
  private var upstreamWeights = Map.empty[TaskId, Double]

  val taskCount = conf.getInt(PageRankApplication.COUNT).get
  lazy val allTasks = (0 until taskCount).toList.map(TaskId(processorId = 1, _))

  private val graph = conf.getValue[Graph[NodeWithTaskId[_], AnyRef]](PageRankApplication.DAG).get

  private val node = graph.getVertices.find { node =>
    node.taskId == taskContext.taskId.index
  }.get

  private val downstream = graph.outgoingEdgesOf(node).map(_._3)
    .map(id => taskId.copy(index = id.taskId)).toSeq
  private val upstreamCount = graph.incomingEdgesOf(node).map(_._1).size

  LOG.info(s"downstream nodes: $downstream")

  private var tick = 0

  private def output(msg: AnyRef, tasks: TaskId*): Unit = {
    taskContext.asInstanceOf[TaskWrapper].outputUnManaged(msg, tasks: _*)
  }

  override def receiveUnManagedMessage: Receive = {
    case Tick(tick) =>
      this.tick = tick

      if (downstream.length == 0) {
        // If there is no downstream, we will evenly distribute our page rank to
        // every node in the graph
        val update = UpdateWeight(taskId, weight / taskCount)
        output(update, allTasks: _*)
      } else {
        val update = UpdateWeight(taskId, weight / downstream.length)
        output(update, downstream: _*)
      }
    case update@UpdateWeight(upstreamTaskId, weight) =>
      upstreamWeights += upstreamTaskId -> weight
      if (upstreamWeights.size == upstreamCount) {
        val nextWeight = upstreamWeights.foldLeft(0.0) { (sum, item) => sum + item._2 }
        this.upstreamWeights = Map.empty[TaskId, Double]
        this.weight = nextWeight
        output(LatestWeight(taskId, weight, tick), TaskId(0, 0))
      }
  }
}

object PageRankWorker {
  case class UpdateWeight(taskId: TaskId, weight: Double)
  case class LatestWeight(taskId: TaskId, weight: Double, tick: Int)
}