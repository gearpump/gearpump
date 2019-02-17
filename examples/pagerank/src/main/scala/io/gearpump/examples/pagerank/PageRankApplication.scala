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
package io.gearpump.examples.pagerank

import akka.actor.ActorSystem
import io.gearpump.cluster.{Application, ApplicationMaster, UserConfig}
import io.gearpump.examples.pagerank.PageRankApplication.NodeWithTaskId
import io.gearpump.streaming.{Processor, StreamApplication}
import io.gearpump.streaming.appmaster.AppMaster
import io.gearpump.streaming.partitioner.HashPartitioner
import io.gearpump.util.Graph
import io.gearpump.util.Graph.Node

/**
 *
 * A simple and naive pagerank implementation.
 *
 * @param name name of the application
 * @param iteration max iteration count
 * @param delta decide the accuracy when the page rank example stops.
 * @param dag  the page rank graph
 */
class PageRankApplication[T](
    override val name: String, iteration: Int, delta: Double, dag: Graph[T, _])
  extends Application {

  override def appMaster: Class[_ <: ApplicationMaster] = classOf[AppMaster]
  override def userConfig(implicit system: ActorSystem): UserConfig = {

    // Map node with taskId
    var taskId = 0
    val pageRankDag = dag.mapVertex { node =>
      val updatedNode = NodeWithTaskId(taskId, node)
      taskId += 1
      updatedNode
    }

    val taskCount = taskId

    val userConfig = UserConfig.empty.withValue(PageRankApplication.DAG, pageRankDag).
      withInt(PageRankApplication.ITERATION, iteration).
      withInt(PageRankApplication.COUNT, taskCount).
      withDouble(PageRankApplication.DELTA, delta)

    val controller = Processor[PageRankController](1)
    val pageRankWorker = Processor[PageRankWorker](taskCount)
    val partitioner = new HashPartitioner

    val app = StreamApplication(name, Graph(controller ~ partitioner ~> pageRankWorker), userConfig)
    app.userConfig
  }
}

object PageRankApplication {
  val DAG = "PageRank.DAG"
  val ITERATION = "PageRank.Iteration"
  val COUNT = "PageRank.COUNT"
  val DELTA = "PageRank.DELTA"
  val REPORTER = "PageRank.Reporter"
  case class NodeWithTaskId[T](taskId: Int, node: T)
}
