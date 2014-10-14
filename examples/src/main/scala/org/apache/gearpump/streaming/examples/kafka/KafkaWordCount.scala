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

package org.apache.gearpump.streaming.examples.kafka

import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.client.ClientContext
import org.apache.gearpump.streaming.transaction.kafka.KafkaConfig
import org.apache.gearpump.streaming.{AppDescription, TaskDescription}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{Configs, Graph}

class KafkaWordCount {
  def getApplication(config: Configs, kafkaSpoutNum: Int, splitNum: Int,
                     sumNum: Int, kafkaBoltNum: Int) : AppDescription = {
    val partitioner = new HashPartitioner()
    val kafkaSpout = TaskDescription(classOf[KafkaSpout], kafkaSpoutNum)
    val split = TaskDescription(classOf[Split], splitNum)
    val sum = TaskDescription(classOf[Sum], sumNum)
    val kafkaBolt = TaskDescription(classOf[KafkaBolt], kafkaBoltNum)
    val app = AppDescription("KafkaWordCount", config,
      Graph(kafkaSpout ~ partitioner ~> split ~ partitioner ~> sum ~ partitioner ~> kafkaBolt))
    app
  }
}

object KafkaWordCount extends App with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    "kafka_spout" -> CLIOption[Int]("<hom many kafka spout tasks>", required = false, defaultValue = Some(1)),
    "split" -> CLIOption[Int]("<how many split tasks>", required = false, defaultValue = Some(4)),
    "sum" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(4)),
    "kafka_bolt" -> CLIOption[Int]("<hom many kafka bolt tasks", required = false, defaultValue = Some(4)),
    "runseconds"-> CLIOption[Int]("<how long to run this example>", required = false, defaultValue = Some(60)))
  val config = parse(args)

  def start(): Unit = {

    val masters = config.getString("master")
    Console.out.println("Master URL: " + masters)

    val context = ClientContext(masters)

    val appId = context.submit(new KafkaWordCount().getApplication(
      Configs(KafkaConfig()), config.getInt("kafka_spout"), config.getInt("split"),
      config.getInt("sum"), config.getInt("kafka_bolt")))

    System.out.println(s"We get application id: $appId")

    Thread.sleep(config.getInt("runseconds") * 1000)

    System.out.println(s"Shutting down application $appId")

    context.shutdown(appId)
    context.destroy()
  }

  start()
}
