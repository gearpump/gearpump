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
package io.gearpump.streaming.examples.fsio

import io.gearpump.streaming.{StreamApplication, Processor}
import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import io.gearpump.partitioner.ShufflePartitioner
import io.gearpump.util.Graph._
import HadoopConfig._
import io.gearpump.util.{AkkaApp, Graph, LogUtil}
import org.apache.hadoop.conf.Configuration
import org.slf4j.Logger

object SequenceFileIO extends AkkaApp with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "source"-> CLIOption[Int]("<sequence file reader number>", required = false, defaultValue = Some(1)),
    "sink"-> CLIOption[Int]("<sequence file writer number>", required = false, defaultValue = Some(1)),
    "input"-> CLIOption[String]("<input file path>", required = true),
    "output"-> CLIOption[String]("<output file directory>", required = true)
  )

  def application(config: ParseResult) : StreamApplication = {
    val spoutNum = config.getInt("source")
    val boltNum = config.getInt("sink")
    val input = config.getString("input")
    val output = config.getString("output")
    val appConfig = UserConfig.empty.withString(SeqFileStreamProducer.INPUT_PATH, input).withString(SeqFileStreamProcessor.OUTPUT_PATH, output)
    val hadoopConfig = appConfig.withHadoopConf(new Configuration())
    val partitioner = new ShufflePartitioner()
    val streamProducer = Processor[SeqFileStreamProducer](spoutNum)
    val streamProcessor = Processor[SeqFileStreamProcessor](boltNum)

    val app = StreamApplication("SequenceFileIO", Graph(streamProducer ~ partitioner ~> streamProcessor), hadoopConfig)
    app
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    val context = ClientContext(akkaConf)
    val appId = context.submit(application(config))
    context.close()
  }
}
