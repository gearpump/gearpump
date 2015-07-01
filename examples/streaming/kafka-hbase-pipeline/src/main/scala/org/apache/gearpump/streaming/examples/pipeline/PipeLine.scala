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
import org.apache.gearpump.external.hbase.{HBaseRepo, HBaseSinkInterface, HBaseSink}
import org.apache.gearpump.external.hbase.HBaseSink._
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{Graph, LogUtil}
import org.apache.hadoop.conf.Configuration
import org.slf4j.Logger

object PipeLine extends App with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  val PROCESSORS = "pipeline.processors"
  val PERSISTORS = "pipeline.persistors"

  override val options: Array[(String, CLIOption[Any])] = Array(
    "conf" -> CLIOption[String]("<conf file>", required = true)
  )

  def application(config: ParseResult): StreamApplication = {
    import Messages._
    val pipeLinePath = config.getString("conf")
    val pipelineConfig = PipeLineConfig(ConfigFactory.parseFile(new java.io.File(pipeLinePath)))
    val processors = pipelineConfig.config.getInt(PROCESSORS)
    val persistors = pipelineConfig.config.getInt(PERSISTORS)
    val kafkaConfig = new KafkaConfig(pipelineConfig.config)
    val repo = new HBaseRepo {
      def getHBase(table: String, conf: Configuration): HBaseSinkInterface = HBaseSink(table, conf)
    }
    val appConfig = UserConfig.empty.withValue(KafkaConfig.NAME, kafkaConfig).withValue(PIPELINE, pipelineConfig).withValue(HBASESINK, repo)

    val kafka = Processor[KafkaProducer](1, "KafkaProducer")
    val cpuProcessor = Processor[CpuProcessor](processors, "CpuProcessor")
    val memoryProcessor = Processor[MemoryProcessor](processors, "MemoryProcessor")
    val cpuPersistor = Processor[CpuPersistor](persistors, "CpuPersistor")
    val memoryPersistor = Processor[MemoryPersistor](persistors, "MemoryPersistor")
    val app = StreamApplication("PipeLine", Graph(
      kafka ~> cpuProcessor ~> cpuPersistor,
      kafka ~> memoryProcessor ~> memoryPersistor
    ), appConfig)
    app
  }

  val config = parse(args)
  val context = ClientContext()
  implicit val system = context.system
  val appId = context.submit(application(config))
  context.close()

}