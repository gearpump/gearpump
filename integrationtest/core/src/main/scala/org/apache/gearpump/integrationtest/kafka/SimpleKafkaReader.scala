/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.integrationtest.kafka

import scala.util.{Failure, Success}

import kafka.api.FetchRequestBuilder
import kafka.consumer.SimpleConsumer
import kafka.utils.Utils

import org.apache.gearpump.streaming.serializer.ChillSerializer

class SimpleKafkaReader(verifier: ResultVerifier, topic: String, partition: Int = 0,
    host: String, port: Int) {

  private val consumer = new SimpleConsumer(host, port, 100000, 64 * 1024, "")
  private val serializer = new ChillSerializer[Int]
  private var offset = 0L

  def read(): Unit = {
    val messageSet = consumer.fetch(
      new FetchRequestBuilder().addFetch(topic, partition, offset, Int.MaxValue).build()
    ).messageSet(topic, partition)

    for (messageAndOffset <- messageSet) {
      serializer.deserialize(Utils.readBytes(messageAndOffset.message.payload)) match {
        case Success(msg) =>
          offset = messageAndOffset.nextOffset
          verifier.onNext(msg)
        case Failure(e) => throw e
      }
    }
  }
}