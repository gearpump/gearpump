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
package io.gearpump.cluster.main

import io.gearpump.cluster.client.ClientContext
import io.gearpump.util.MasterClientCommand

// Internal tool to restart an application
object Replay extends MasterClientCommand with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array(
    "appid" -> CLIOption("<application id>", required = true),
    // For document purpose only, OPTION_CONFIG option is not used here.
    // OPTION_CONFIG is parsed by parent shell command "Gear" transparently.
    Gear.OPTION_CONFIG -> CLIOption("custom configuration file", required = false,
      defaultValue = None))

  override val description = "Replay the application from current min clock(low watermark)"

  def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)

    if (null != config) {
      val client = ClientContext(akkaConf)
      client.replayFromTimestampWindowTrailingEdge(config.getInt("appid"))
      client.close()
    }
  }
}
