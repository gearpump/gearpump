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

import org.apache.gearpump.streaming.client.ClientContext
import org.apache.gearpump.streaming.examples.kafka.KafkaWordCount
import org.apache.gearpump.streaming.examples.sol._
import org.apache.gearpump.streaming.examples.wordcount.WordCount
import org.apache.gearpump.util.Configs

val context = ClientContext(System.getProperty("masterActorPath"))

class Example {
  def sol(streamProducer : Int, streamProcessor : Int, bytesPerMessage: Int, stages: Int) =
    SOL.getApplication(streamProducer,
                       streamProcessor, bytesPerMessage, stages)
  def wordcount(split:Int, sum:Int) = new WordCount().getApplication(split, sum)
  def kafkawordcount(conf: Configs, kafkaSpout: Int, split: Int, sum: Int, kafkaBolt: Int) =
    new KafkaWordCount().getApplication(conf, kafkaSpout, split, sum, kafkaBolt)
}

val example = new Example