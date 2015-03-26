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

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    "kafka" -> CLIOption[Int]("<how many kafka tasks>", required = false, defaultValue = Some(4)),
    "hbase" -> CLIOption[Int]("<how many hbase tasks>", required = false, defaultValue = Some(4)),
    "table" -> CLIOption[String]("<hbase table name>", required = true),
    "cf" -> CLIOption[String]("<column family of hbase table>", required = true)
  )

  def application(config: ParseResult) : AppDescription = {
    val kafkaNum = config.getInt("kafka")
    val hbaseNum = config.getInt("hbase")
    val tableName = config.getString("table")
    val columnFamily = config.getString("cf")
    val partitioner = new HashPartitioner()
    val kafka = TaskDescription(classOf[Kafka].getName, kafkaNum)
    val hbase = TaskDescription(classOf[HBaseSink].getName, hbaseNum)
    val kafkaConfig = KafkaConfig(ConfigFactory.parseResources("kafka.conf"))
    val userConfig = UserConfig.empty.withString(HBaseSink.TABLE_NAME, tableName).
      withString(HBaseSink.TABLE_COLUMN_FAMILY, columnFamily).withValue(KafkaConfig.NAME, kafkaConfig)

    val app = AppDescription("pipeLine", userConfig, Graph(kafka ~ partitioner ~> hbase))
    app
  }

  val config = parse(args)
  val context = ClientContext(config.getString("master"))
  implicit val system = context.system
  val appId = context.submit(application(config))
  context.close()
}

