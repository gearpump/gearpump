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
package org.apache.gearpump.streaming.examples.fsio

import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.partitioner.ShufflePartitioner
import org.apache.gearpump.streaming.{AppMaster, AppDescription, TaskDescription}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{HadoopConfig, LogUtil, Configs, Graph}
import org.apache.hadoop.conf.Configuration
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

object SequenceFileIO extends App with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    "source"-> CLIOption[Int]("<sequence file reader number>", required = false, defaultValue = Some(2)),
    "sink"-> CLIOption[Int]("<sequence file writer number>", required = false, defaultValue = Some(2)),
    "runseconds" -> CLIOption[Int]("<run seconds>", required = false, defaultValue = Some(60)),
    "input"-> CLIOption[String]("<input file path>", required = true),
    "output"-> CLIOption[String]("<output file directory>", required = true)
  )

  def application(config: ParseResult) : AppDescription = {
    val spoutNum = config.getInt("source")
    val boltNum = config.getInt("sink")
    val input = config.getString("input")
    val output = config.getString("output")
    val appConfig = Configs(ConfigFactory.load("fsio.conf")).withValue(SeqFileStreamProducer.INPUT_PATH, input).withValue(SeqFileStreamProcessor.OUTPUT_PATH, output)
    val hadoopConfig = HadoopConfig(appConfig).withHadoopConf(new Configuration())
    val partitioner = new ShufflePartitioner()
    val streamProducer = TaskDescription(classOf[SeqFileStreamProducer].getCanonicalName, spoutNum)
    val streamProcessor = TaskDescription(classOf[SeqFileStreamProcessor].getCanonicalName, boltNum)
    LOG.info(s"SequenceFileIO appConfig size = ${appConfig.config.size}")
    val app = AppDescription("SequenceFileIO", classOf[AppMaster].getCanonicalName, hadoopConfig, Graph(streamProducer ~ partitioner ~> streamProcessor))
    app
  }

  val config = parse(args)
  val context = ClientContext(config.getString("master"))
  val appId = context.submit(application(config))
  Thread.sleep(config.getInt("runseconds") * 1000)
  context.shutdown(appId)
  context.cleanup()
}
