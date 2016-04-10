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
package io.gearpump.integrationtest.storm

import backtype.storm.topology.{OutputFieldsDeclarer, BasicOutputCollector}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.{Fields, Values, Tuple}
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper

class Adaptor extends BaseBasicBolt {
  private var id = 0L

  override def execute(tuple: Tuple, collector: BasicOutputCollector): Unit = {
    val bytes = tuple.getBinary(0)
    collector.emit(new Values(s"$id".getBytes, bytes))
    id += 1
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields(FieldNameBasedTupleToKafkaMapper.BOLT_KEY,
      FieldNameBasedTupleToKafkaMapper.BOLT_MESSAGE))
  }
}
