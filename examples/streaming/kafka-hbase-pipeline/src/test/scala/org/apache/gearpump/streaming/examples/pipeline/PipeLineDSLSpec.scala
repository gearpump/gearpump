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

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.external.hbase.dsl.HBaseDSLSink._
import org.apache.gearpump.streaming.dsl.StreamApp
import org.apache.gearpump.streaming.examples.pipeline.Messages.{Body, Datum, Envelope, _}
import org.apache.gearpump.streaming.kafka.dsl.KafkaDSLUtil
import org.apache.gearpump.streaming.kafka.lib.{KafkaSourceConfig, KafkaUtil}
import org.apache.gearpump.streaming.transaction.api.OffsetStorageFactory
import org.apache.gearpump.util.LogUtil
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfterAll, Matchers, PropSpec}
import org.slf4j.Logger
import upickle._

class PipeLineDSLSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar with BeforeAndAfterAll {
  val LOG: Logger = LogUtil.getLogger(getClass)
  val PROCESSORS = "pipeline.processors"
  val PERSISTORS = "pipeline.persistors"
  val pipeLinePath = "conf/pipeline.conf.template"
  val pipeLineConfig = ConfigFactory.parseFile(new java.io.File(pipeLinePath))
  val kafkaConfig = new KafkaSourceConfig(KafkaUtil.buildConsumerConfig("localhost:2181"))

  implicit var system: ActorSystem = null

  override def beforeAll: Unit = {
    system = ActorSystem("PipeLineDSLSpec")
  }

  override def afterAll: Unit = {
    system.shutdown()
  }


  property("StreamApp should readFromKafka") {
    val app = new StreamApp("PipeLineDSL", system, UserConfig.empty)
    val offsetStorageFactory = mock[OffsetStorageFactory]
    val producer = KafkaDSLUtil.createStream[String](app, 1, "", kafkaConfig, offsetStorageFactory).map{ message =>
      val envelope = read[Envelope](message)
      val body = read[Body](envelope.body)
      body.metrics
    }.flatMap(metrics => {
      Some(metrics.flatMap(datum => {
        datum.dimension match {
          case CPU =>
            Some(datum)
          case _ =>
            None
        }
      }))
    }).reduce((() => {
      val average = TAverage(pipeLineConfig.getInt(CPU_INTERVAL))
      (msg1: Array[Datum], msg2: Array[Datum]) => {
        val now = System.currentTimeMillis
        msg2.flatMap(datum => {
          average.average(datum, now)
        })
      }
    })()).writeToHbase("mock", 1, "sink")
    val graphVertices = List(
      "org.apache.gearpump.streaming.dsl.op.DataSourceOp",
      "org.apache.gearpump.streaming.dsl.op.FlatMapOp",
      "org.apache.gearpump.streaming.dsl.op.FlatMapOp",
      "org.apache.gearpump.streaming.dsl.op.ReduceOp",
      "org.apache.gearpump.streaming.dsl.op.DataSinkOp"
    )
    var i = 0
    app.graph.vertices.foreach(op => {
      //LOG.info(s"${graphVertices(i)} ${op.getClass.getName}")
      assert(graphVertices(i) == op.getClass.getName)
      i = i+1
    })
    app.plan.dag.vertices.foreach(desc => {
      LOG.info(s"taskClass=${desc.taskClass}")
    })
  }
}

