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

import kafka.consumer.ConsumerConfig
import org.apache.gearpump.cluster.main.{CLIOption, ArgumentsParser}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.client.ClientContext
import org.apache.gearpump.streaming.{TaskDescription, AppDescription}
import org.apache.gearpump.util.{Graph, Configs}
import org.apache.gearpump.util.Graph._

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

  import KafkaConstants._

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    ZOOKEEPER -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    KAFKA_ROOT -> CLIOption[String]("<Kafka root path on Zookeeper>", required = true),
    CONSUMER_TOPIC -> CLIOption[String]("<Kafka consumer topic>", required = true),
    CLIENT_ID -> CLIOption[String]("<client id to identify the application>", required = false,
      defaultValue = Some("gearpump-app")),
    SO_TIMEOUT -> CLIOption[Int]("<socket timeout for network requests>", required = false,
      defaultValue = Some(ConsumerConfig.SocketTimeout)),
    SO_BUFFERSIZE -> CLIOption[Int]("<socket receive buffer for network requests", required = false,
      defaultValue = Some(ConsumerConfig.SocketBufferSize)),
    FETCH_SIZE -> CLIOption[Int]("<number of bytes of message to fetch in each request>", required = false,
      defaultValue = Some(ConsumerConfig.FetchSize)),
    PRODUCER_TOPIC -> CLIOption[String]("<Kafka producer topic", required = true),
    BROKER_LIST -> CLIOption[String]("<Kafka broker list", required = true),
    PRODUCER_TYPE -> CLIOption[String]("<whether Kafka producer send messages asynchronously>", required = false,
      defaultValue = Some("sync")),
    SERIALIZER_CLASS -> CLIOption[String]("<serializer class for Kafka producer messages>", required = false,
      defaultValue = Some("kafka.serializer.StringEncoder")),
    REQUIRED_ACKS -> CLIOption[String]("when a produce request is considered complete", required = false,
      defaultValue = Some("1")),
    BATCH_SIZE -> CLIOption[Int]("<number of messages to emit in each output>", required = false,
      defaultValue = Some(100)),
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
      Configs(Map(
      ZOOKEEPER -> config.getString(ZOOKEEPER),
      KAFKA_ROOT -> config.getString(KAFKA_ROOT),
      CONSUMER_TOPIC -> config.getString(CONSUMER_TOPIC),
      CLIENT_ID -> config.getString(CLIENT_ID),
      SO_TIMEOUT -> config.getInt(SO_TIMEOUT),
      SO_BUFFERSIZE -> config.getInt(SO_BUFFERSIZE),
      FETCH_SIZE -> config.getInt(FETCH_SIZE),
      PRODUCER_TOPIC -> config.getString(PRODUCER_TOPIC),
      BROKER_LIST -> config.getString(BROKER_LIST),
      PRODUCER_TYPE -> config.getString(PRODUCER_TYPE),
      SERIALIZER_CLASS -> config.getString(SERIALIZER_CLASS),
      REQUIRED_ACKS -> config.getString(REQUIRED_ACKS),
      BATCH_SIZE -> config.getInt(BATCH_SIZE)
      )),
      config.getInt("kafka_spout"), config.getInt("split"), config.getInt("sum"), config.getInt("kafka_bolt")))

    System.out.println(s"We get application id: $appId")

    Thread.sleep(config.getInt("runseconds") * 1000)

    System.out.println(s"Shutting down application $appId")

    context.shutdown(appId)
    context.destroy()
  }

  start()
}
