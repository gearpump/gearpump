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

package io.gearpump.streaming.examples.state

import akka.actor.ActorSystem
import io.gearpump.streaming.{StreamApplication, Processor}
import io.gearpump.streaming.examples.state.processor.{WindowAverageProcessor, NumberGeneratorProcessor}
import io.gearpump.streaming.hadoop.HadoopCheckpointStoreFactory
import io.gearpump.streaming.state.impl.{WindowConfig, PersistentStateConfig}
import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import io.gearpump.partitioner.HashPartitioner
import io.gearpump.util.Graph.Node
import io.gearpump.util.{AkkaApp, Graph}
import org.apache.hadoop.conf.Configuration

object WindowAverageApp extends AkkaApp with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array(
    "gen" -> CLIOption("<how many gen tasks>", required = false, defaultValue = Some(1)),
    "window" -> CLIOption("<how mange window tasks", required = false, defaultValue = Some(1)),
    "window_size" -> CLIOption("<window size in milliseconds>", required = false , defaultValue = Some(5000)),
    "window_step" -> CLIOption("<window step in milliseconds>", required = false , defaultValue = Some(5000))
  )

  def application(config: ParseResult)(implicit system: ActorSystem) : StreamApplication = {
    val windowSize = config.getInt("window_size")
    val windowStep = config.getInt("window_step")
    val checkpointStoreFactory = new HadoopCheckpointStoreFactory("MessageCount", new Configuration)
    val taskConfig = UserConfig.empty
        .withBoolean(PersistentStateConfig.STATE_CHECKPOINT_ENABLE, true)
        .withLong(PersistentStateConfig.STATE_CHECKPOINT_INTERVAL_MS, 1000L)
        .withValue(PersistentStateConfig.STATE_CHECKPOINT_STORE_FACTORY, checkpointStoreFactory)
        .withValue(WindowConfig.NAME, WindowConfig(windowSize, windowStep))
    val gen = Processor[NumberGeneratorProcessor](config.getInt("gen"))
    val count = Processor[WindowAverageProcessor](config.getInt("window"), taskConf = taskConfig)
    val partitioner = new HashPartitioner()
    val app = StreamApplication("WindowAverage", Graph(gen ~ partitioner ~> count), UserConfig.empty)
    app
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    val context = ClientContext(akkaConf)

    implicit val system = context.system
    val appId = context.submit(application(config))
    context.close()
  }
}
