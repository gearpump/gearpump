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
package io.gearpump.integrationtest.kafka

import java.util.Properties

import io.gearpump.streaming.serializer.ChillSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.log4j.Logger

class NumericalDataProducer(topic: String, bootstrapServers: String) {

  private val LOG = Logger.getLogger(getClass)
  private val producer = createProducer
  private val WRITE_SLEEP_NANOS = 10
  private val serializer = new ChillSerializer[Int]
  var lastWriteNum = 0

  def start(): Unit = {
    produceThread.start()
  }

  def stop(): Unit = {
    if (produceThread.isAlive) {
      produceThread.interrupt()
      produceThread.join()
    }
    producer.close()
  }

  private def createProducer: KafkaProducer[Array[Byte], Array[Byte]] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", bootstrapServers)
    new KafkaProducer[Array[Byte], Array[Byte]](properties, new ByteArraySerializer, new ByteArraySerializer)
  }

  private val produceThread = new Thread(new Runnable {
    override def run(): Unit = {
      try {
        while (!Thread.currentThread.isInterrupted) {
          lastWriteNum += 1
          val msg = serializer.serialize(lastWriteNum)
          val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, msg)
          producer.send(record)
          Thread.sleep(0, WRITE_SLEEP_NANOS)
        }
      } catch {
        case ex: InterruptedException =>
          LOG.info("message producing is stopped by an interrupt")
      }
    }
  })

}
