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

package org.apache.gearpump.streaming.examples.complexdag

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.streaming.{AppDescription, TaskDescription}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{Graph, LogUtil}
import org.slf4j.Logger

/*
 digraph flow {
     Source_0 -> Sink_0;
     Source_0 -> Sink_1;
     Source_0 -> Sink_2;
     Source_0 -> Node_1;
     Source_1 -> Node_0;
     Node_0 -> Sink_3;
     Node_1 -> Sink_3;
     Node_1 -> Sink_4;
     Node_1 -> Node_4;
     Node_2 -> Node_3;
     Node_1 -> Node_3;
     Source_0 -> Node_2;
     Source_0 -> Node_3;
     Node_3 -> Sink_3;
     Node_4 -> Sink_3;
     Source_1 -> Sink_4;
 }
*/
case class Source_0(_context : TaskContext, _conf: UserConfig) extends Source(_context, _conf)
case class Source_1(_context : TaskContext, _conf: UserConfig) extends Source(_context, _conf)
case class Node_0(_context : TaskContext, _conf: UserConfig) extends Node(_context, _conf)
case class Node_1(_context : TaskContext, _conf: UserConfig) extends Node(_context, _conf)
case class Node_2(_context : TaskContext, _conf: UserConfig) extends Node(_context, _conf)
case class Node_3(_context : TaskContext, _conf: UserConfig) extends Node(_context, _conf)
case class Node_4(_context : TaskContext, _conf: UserConfig) extends Node(_context, _conf)
case class Sink_0(_context : TaskContext, _conf: UserConfig) extends Sink(_context, _conf)
case class Sink_1(_context : TaskContext, _conf: UserConfig) extends Sink(_context, _conf)
case class Sink_2(_context : TaskContext, _conf: UserConfig) extends Sink(_context, _conf)
case class Sink_3(_context : TaskContext, _conf: UserConfig) extends Sink(_context, _conf)
case class Sink_4(_context : TaskContext, _conf: UserConfig) extends Sink(_context, _conf)

object Dag extends App with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  val RUN_FOR_EVER = -1

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    "runseconds"-> CLIOption[Int]("<how long to run this example, set to -1 if run forever>", required = false, defaultValue = Some(60))
  )

  def application(config: ParseResult) : AppDescription = {
    val partitioner = new HashPartitioner()
    val source_0 = TaskDescription(classOf[Source_0].getName, 1)
    val source_1 = TaskDescription(classOf[Source_1].getName, 1)
    val node_0 = TaskDescription(classOf[Node_0].getName, 1)
    val node_1 = TaskDescription(classOf[Node_1].getName, 1)
    val node_2 = TaskDescription(classOf[Node_2].getName, 1)
    val node_3 = TaskDescription(classOf[Node_3].getName, 1)
    val node_4 = TaskDescription(classOf[Node_4].getName, 1)
    val sink_0 = TaskDescription(classOf[Sink_0].getName, 1)
    val sink_1 = TaskDescription(classOf[Sink_1].getName, 1)
    val sink_2 = TaskDescription(classOf[Sink_2].getName, 1)
    val sink_3 = TaskDescription(classOf[Sink_3].getName, 1)
    val sink_4 = TaskDescription(classOf[Sink_4].getName, 1)
    val app = AppDescription("dag", UserConfig.empty, Graph(
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
    ))
    app
  }

  val config = parse(args)
  val context = ClientContext(config.getString("master"))
  implicit val system = context.system
  val appId = context.submit(application(config))
  Thread.sleep(config.getInt("runseconds") * 1000)
  context.shutdown(appId)
  context.close()
}

