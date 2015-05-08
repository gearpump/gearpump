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
package gearpump.streaming.examples.fsio

import gearpump.cluster.main.ArgumentsParser
import gearpump.streaming.{StreamApplication, Processor}
import gearpump.cluster.UserConfig
import gearpump.cluster.client.ClientContext
import gearpump.cluster.main.{ParseResult, CLIOption, ArgumentsParser}
import gearpump.partitioner.ShufflePartitioner
import gearpump.util.Graph._
import gearpump.util.HadoopConfig._
import gearpump.util.{Graph, LogUtil}
import org.apache.hadoop.conf.Configuration
import org.slf4j.Logger

object SequenceFileIO extends App with ArgumentsParser {
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

  val config = parse(args)
  val context = ClientContext()
  val appId = context.submit(application(config))
  context.close()
}
