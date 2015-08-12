/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.examples.state

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.examples.state.processor.{CountProcessor, NumberGeneratorProcessor}
import org.apache.gearpump.streaming.hadoop.HadoopCheckpointStoreFactory
import org.apache.gearpump.streaming.hadoop.lib.rotation._
import org.apache.gearpump.streaming.state.impl.PersistentStateConfig
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.Graph.Node
import org.apache.gearpump.util.{AkkaApp, Graph}
import org.apache.hadoop.conf.Configuration

object MessageCountApp extends AkkaApp with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array(
    "gen" -> CLIOption("<how many gen tasks>", required = false, defaultValue = Some(1)),
    "count" -> CLIOption("<how mange count tasks", required = false, defaultValue = Some(1))
  )

  def application(config: ParseResult)(implicit system: ActorSystem) : StreamApplication = {
    val hadoopConfig = new Configuration
    hadoopConfig.set("fs.defaultFS", "hdfs://localhost:9000")
    val checkpointStoreFactory = new HadoopCheckpointStoreFactory("MessageCount", hadoopConfig,
      // rotate on 1KB
      new FileSizeRotation(1000))
    val taskConfig = UserConfig.empty
      .withBoolean(PersistentStateConfig.STATE_CHECKPOINT_ENABLE, true)
      .withLong(PersistentStateConfig.STATE_CHECKPOINT_INTERVAL_MS, 1000L)
      .withValue(PersistentStateConfig.STATE_CHECKPOINT_STORE_FACTORY, checkpointStoreFactory)

    val gen = Processor[NumberGeneratorProcessor](config.getInt("gen"))
    val count = Processor[CountProcessor](config.getInt("count"), taskConf = taskConfig)
    val partitioner = new HashPartitioner()
    val app = StreamApplication("MessageCount", Graph(gen ~ partitioner ~> count), UserConfig.empty)
    app
  }

  def main(akkaConf: Config, args: Array[String]): Unit = {

    val config = parse(args)
    val context = ClientContext(akkaConf)
    implicit val system = context.system
    val appId = context.submit(application(config))
    context.close()
  }
}
