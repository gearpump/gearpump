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

package io.gearpump.streaming.dsl.scalaapi

import akka.actor.ActorSystem
import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.client.ClientContext
import io.gearpump.streaming.StreamApplication
import io.gearpump.streaming.dsl.plan._
import io.gearpump.streaming.source.{DataSource, Watermark}
import io.gearpump.streaming.task.TaskContext
import io.gearpump.util.Graph
import java.time.Instant
import scala.language.implicitConversions

/**
 * Example:
 * {{{
 * val data = "This is a good start, bingo!! bingo!!"
 * app.fromCollection(data.lines.toList).
 * // word => (word, count)
 * flatMap(line => line.split("[\\s]+")).map((_, 1)).
 * // (word, count1), (word, count2) => (word, count1 + count2)
 * groupBy(kv => kv._1).reduce(sum(_, _))
 *
 * val appId = context.submit(app)
 * context.close()
 * }}}
 *
 * @param name name of app
 */
class StreamApp(
    name: String, system: ActorSystem, userConfig: UserConfig,
    private val graph: Graph[Op, OpEdge]) {

  def this(name: String, system: ActorSystem, userConfig: UserConfig) = {
    this(name, system, userConfig, Graph.empty[Op, OpEdge])
  }

  def plan(): StreamApplication = {
    implicit val actorSystem = system
    val planner = new Planner
    val dag = planner.plan(graph)
    StreamApplication(name, dag, userConfig)
  }

  def source[T](dataSource: DataSource, parallelism: Int = 1,
      conf: UserConfig = UserConfig.empty, description: String = "source"): Stream[T] = {
    val sourceOp = DataSourceOp(dataSource, parallelism, description, conf)
    graph.addVertex(sourceOp)
    new Stream[T](graph, sourceOp)
  }

  def source[T](seq: Seq[T], parallelism: Int, description: String): Stream[T] = {
    this.source(new CollectionDataSource[T](seq), parallelism, UserConfig.empty, description)
  }
}

object StreamApp {
  def apply(name: String, context: ClientContext, userConfig: UserConfig = UserConfig.empty)
    : StreamApp = {
    new StreamApp(name, context.system, userConfig)
  }

  implicit def streamAppToApplication(streamApp: StreamApp): StreamApplication = {
    streamApp.plan()
  }
}

/** A test message source which generated message sequence repeatedly. */
class CollectionDataSource[T](seq: Seq[T]) extends DataSource {
  private lazy val iterator: Iterator[T] = seq.iterator

  override def open(context: TaskContext, startTime: Instant): Unit = {}

  override def read(): Message = {
    if (iterator.hasNext) {
      Message(iterator.next(), Instant.now())
    } else {
      null
    }
  }

  override def close(): Unit = {}

  override def getWatermark: Instant = {
    if (iterator.hasNext) {
      Instant.now()
    } else {
      Watermark.MAX
    }
  }
}