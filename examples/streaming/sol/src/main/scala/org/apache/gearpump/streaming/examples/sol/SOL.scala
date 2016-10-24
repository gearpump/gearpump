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

package org.apache.gearpump.streaming.examples.sol

import org.slf4j.Logger

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.streaming.partitioner.ShufflePartitioner
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{AkkaApp, Graph, LogUtil}

object SOL extends AkkaApp with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "streamProducer" -> CLIOption[Int]("<stream producer number>", required = false,
    defaultValue = Some(1)),
    "streamProcessor" -> CLIOption[Int]("<stream processor number>", required = false,
    defaultValue = Some(1)),
    "bytesPerMessage" -> CLIOption[Int]("<size of each message>", required = false,
    defaultValue = Some(100)),
    "stages" -> CLIOption[Int]("<how many stages to run>", required = false,
    defaultValue = Some(2)))

  def application(config: ParseResult): StreamApplication = {
    val spoutNum = config.getInt("streamProducer")
    val boltNum = config.getInt("streamProcessor")
    val bytesPerMessage = config.getInt("bytesPerMessage")
    val stages = config.getInt("stages")
    val appConfig = UserConfig.empty.withInt(SOLStreamProducer.BYTES_PER_MESSAGE, bytesPerMessage)
    val partitioner = new ShufflePartitioner()
    val streamProducer = Processor[SOLStreamProducer](spoutNum)
    val streamProcessor = Processor[SOLStreamProcessor](boltNum)
    var computation = streamProducer ~ partitioner ~> streamProcessor
    computation = 0.until(stages - 2).foldLeft(computation) { (c, id) =>
      c ~ partitioner ~> streamProcessor.copy()
    }
    val dag = Graph(computation)
    val app = StreamApplication("sol", dag, appConfig)
    app
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    val context = ClientContext(akkaConf)
    val appId = context.submit(application(config))
    context.close()
  }
}
