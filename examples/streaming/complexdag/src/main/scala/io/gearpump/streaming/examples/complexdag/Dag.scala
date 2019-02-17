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

package io.gearpump.streaming.examples.complexdag

import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import io.gearpump.streaming.{Processor, StreamApplication}
import io.gearpump.streaming.partitioner.HashPartitioner
import io.gearpump.streaming.task.TaskContext
import io.gearpump.util.{AkkaApp, Graph}
import io.gearpump.util.Graph.{Node => GraphNode}

case class Source_0(_context: TaskContext, _conf: UserConfig) extends Source(_context, _conf)
case class Source_1(_context: TaskContext, _conf: UserConfig) extends Source(_context, _conf)
case class Node_0(_context: TaskContext, _conf: UserConfig) extends Node(_context, _conf)
case class Node_1(_context: TaskContext, _conf: UserConfig) extends Node(_context, _conf)
case class Node_2(_context: TaskContext, _conf: UserConfig) extends Node(_context, _conf)
case class Node_3(_context: TaskContext, _conf: UserConfig) extends Node(_context, _conf)
case class Node_4(_context: TaskContext, _conf: UserConfig) extends Node(_context, _conf)
case class Sink_0(_context: TaskContext, _conf: UserConfig) extends Sink(_context, _conf)
case class Sink_1(_context: TaskContext, _conf: UserConfig) extends Sink(_context, _conf)
case class Sink_2(_context: TaskContext, _conf: UserConfig) extends Sink(_context, _conf)
case class Sink_3(_context: TaskContext, _conf: UserConfig) extends Sink(_context, _conf)
case class Sink_4(_context: TaskContext, _conf: UserConfig) extends Sink(_context, _conf)

/**
 * digraph flow {
 *   Source_0 -> Sink_0;
 *   Source_0 -> Sink_1;
 *   Source_0 -> Sink_2;
 *   Source_0 -> Node_1;
 *   Source_1 -> Node_0;
 *   Node_0 -> Sink_3;
 *   Node_1 -> Sink_3;
 *   Node_1 -> Sink_4;
 *   Node_1 -> Node_4;
 *   Node_2 -> Node_3;
 *   Node_1 -> Node_3;
 *   Source_0 -> Node_2;
 *   Source_0 -> Node_3;
 *   Node_3 -> Sink_3;
 *   Node_4 -> Sink_3;
 *   Source_1 -> Sink_4;
 * }
 */
object Dag extends AkkaApp with ArgumentsParser {
  override val options: Array[(String, CLIOption[Any])] = Array.empty

  def application(): StreamApplication = {

    val source_0 = Processor[Source_0](1)
    val source_1 = Processor[Source_1](1)
    val node_0 = Processor[Node_0](1)
    val node_1 = Processor[Node_1](1)
    val node_2 = Processor[Node_2](1)
    val node_3 = Processor[Node_3](1)
    val node_4 = Processor[Node_4](1)
    val sink_0 = Processor[Sink_0](1)
    val sink_1 = Processor[Sink_1](1)
    val sink_2 = Processor[Sink_2](1)
    val sink_3 = Processor[Sink_3](1)
    val sink_4 = Processor[Sink_4](1)
    val partitioner = new HashPartitioner
    val app = StreamApplication("dag", Graph(
      source_0 ~ partitioner ~> sink_1,
      source_0 ~ partitioner ~> sink_2,
      source_0 ~ partitioner ~> node_2,
      source_0 ~ partitioner ~> node_3,
      source_0 ~ partitioner ~> node_1,
      source_0 ~ partitioner ~> sink_0,
      node_2 ~ partitioner ~> node_3,
      node_1 ~ partitioner ~> node_3,
      node_1 ~ partitioner ~> sink_3,
      node_1 ~ partitioner ~> node_4,
      source_1 ~ partitioner ~> sink_4,
      source_1 ~ partitioner ~> node_0,
      node_3 ~ partitioner ~> sink_3,
      node_4 ~ partitioner ~> sink_3,
      node_0 ~ partitioner ~> sink_3
    ), UserConfig.empty)
    app
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val context = ClientContext(akkaConf)
    context.submit(application())
    context.close()
  }
}

