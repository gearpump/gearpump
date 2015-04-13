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

package org.apache.gearpump.streaming.state.example

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ParseResult, CLIOption, ArgumentsParser}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.state.example.processor.{CountProcessor, GenNumProcessor}
import org.apache.gearpump.streaming.state.impl.DefaultCheckpointManager
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.Graph.Node


object MessageCount extends App with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array(
    "gen" -> CLIOption("<how many gen tasks>", required = false, defaultValue = Some(1)),
    "count" -> CLIOption("<how mange count tasks", required = false, defaultValue = Some(1)),
    "checkpoint_interval" -> CLIOption("<interval in milliseconds to checkpoint>", required = false, defaultValue = Some(100))
  )

  def application(config: ParseResult) : StreamApplication = {
    val checkpointInterval = config.getInt("checkpoint_interval")
    val userConfig = UserConfig.empty.withLong(DefaultCheckpointManager.CHECKPOINT_INTERVAL, checkpointInterval)
    val gen = Processor[GenNumProcessor](config.getInt("gen"))
    val count = Processor[CountProcessor](config.getInt("count"))
    val partitioner = new HashPartitioner()
    val app = StreamApplication("MessageCount", Graph(gen ~ partitioner ~> count), userConfig)
    app
  }

  val config = parse(args)
  val context = ClientContext()
  implicit val system = context.system
  val appId = context.submit(application(config))
  context.close()
}
