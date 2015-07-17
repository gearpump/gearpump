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

package org.apache.gearpump.streaming.examples.pipeline

import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.external.hbase.HBaseSink
import org.apache.gearpump.streaming.kafka.KafkaSource
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.sink.DataSinkProcessor
import org.apache.gearpump.streaming.source.DataSourceProcessor
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{AkkaApp, Graph, LogUtil}
import org.slf4j.Logger

object PipeLine extends AkkaApp with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  val PROCESSORS = "pipeline.processors"
  val PERSISTORS = "pipeline.persistors"

  override val options: Array[(String, CLIOption[Any])] = Array(
    "conf" -> CLIOption[String]("<conf file>", required = true)
  )

  def application(context: ClientContext, config: ParseResult): StreamApplication = {
    implicit val system = context.system
    import Messages._
    val pipeLinePath = config.getString("conf")
    val pipelineConfig = PipeLineConfig(ConfigFactory.parseFile(new java.io.File(pipeLinePath)))
    val processors = pipelineConfig.config.getInt(PROCESSORS)
    val kafkaConfig = new KafkaConfig(pipelineConfig.config)
    val appConfig = UserConfig.empty.withValue(KafkaConfig.NAME, kafkaConfig).withValue(PIPELINE, pipelineConfig)

    val tableName = pipelineConfig.config.getString(HBaseSink.TABLE_NAME)

    val kafka = DataSourceProcessor(new KafkaSource(kafkaConfig, new DatumDecoder), 1)
    val cpuProcessor = Processor[CpuProcessor](processors, "CpuProcessor")
    val memoryProcessor = Processor[MemoryProcessor](processors, "MemoryProcessor")
    val hbaseSink = DataSinkProcessor(HBaseSink(tableName), 1)
    val app = StreamApplication("PipeLine", Graph(
      kafka ~> cpuProcessor ~> hbaseSink,
      kafka ~> memoryProcessor ~> hbaseSink
    ), appConfig)
    app
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {

    val config = parse(args)
    val context = ClientContext(akkaConf)

    val appId = context.submit(application(context, config))
    context.close()
  }
}