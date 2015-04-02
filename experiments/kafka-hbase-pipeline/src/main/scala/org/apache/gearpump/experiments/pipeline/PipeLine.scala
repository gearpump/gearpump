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

package org.apache.gearpump.experiments.pipeline

import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.{AppDescription, TaskDescription}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{Graph, LogUtil}
import org.slf4j.Logger

object PipeLine extends App with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  val PROCESSORS = "pipeline.processors"
  val PERSISTORS = "pipeline.persistors"

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    "conf" -> CLIOption[String]("<conf file>", required = true)
  )
  
  Option(parse(args)).foreach(config => {
    val context = ClientContext(config.getString("master"))
    implicit val system = context.system
    def application(config: ParseResult) : AppDescription = {
      import Messages._
      import PipelineConfig._
      val pipeLinePath = config.getString("conf")
      val pipelineConfig = PipelineConfig(ConfigFactory.parseFile(new java.io.File(pipeLinePath)))
      //val clusterConfig = ClusterConfigSource(pipeLinePath)
      val processors = pipelineConfig.getInt(PROCESSORS)
      val persistors = pipelineConfig.getInt(PERSISTORS)
      val kafkaConfig = KafkaConfig(pipelineConfig)
      val appConfig = UserConfig.empty.withValue(KafkaConfig.NAME, kafkaConfig).withValue(PIPELINE, pipelineConfig)
      val partitioner = new HashPartitioner
      val kafka = TaskDescription(classOf[KafkaProducer].getName, 1, "KafkaProducer")
      val cpuProcessor = TaskDescription(classOf[CpuProcessor].getName, processors, "CpuProcessor")
      val memoryProcessor = TaskDescription(classOf[MemoryProcessor].getName, processors, "MemoryProcessor")
      val cpuPersistor = TaskDescription(classOf[CpuPersistor].getName, persistors, "CpuPersistor")
      val memoryPersistor = TaskDescription(classOf[MemoryPersistor].getName, persistors, "MemoryPersistor")
      val app = AppDescription("PipeLine", appConfig, Graph(
        kafka ~ partitioner ~> cpuProcessor ~ partitioner ~> cpuPersistor,
        kafka ~ partitioner ~> memoryProcessor ~ partitioner ~> memoryPersistor
      )/*, clusterConfig*/)
      app
    }
    context.submit(application(config))
    context.close()
  })

}

