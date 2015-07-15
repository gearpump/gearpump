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
import org.apache.gearpump._
import org.apache.gearpump.cluster.UserConfig
import Messages.{Body, Envelope, Datum}
import Messages._
import org.apache.gearpump.external.hbase.{HBaseConsumer, HBaseSinkInterface}
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.dsl.op.OpType._
import org.apache.gearpump.streaming.dsl.plan.OpTranslator.SinkTask
import org.apache.gearpump.streaming.dsl.{SinkConsumer, StreamApp}
import org.apache.gearpump.streaming.examples.pipeline.Messages.{Datum, Body, Envelope}
import org.apache.gearpump.streaming.kafka.dsl.KafkaDSLUtil
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.task.{StartTime, TaskContext}
import org.apache.gearpump.util.{Constants, LogUtil}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfterAll, Matchers, PropSpec}
import org.slf4j.Logger
import upickle._

class PipeLineDSLSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfterAll {
  val LOG: Logger = LogUtil.getLogger(getClass)
  val PROCESSORS = "pipeline.processors"
  val PERSISTORS = "pipeline.persistors"
  val pipeLinePath = "conf/pipeline.conf.template"
  val pipeLineConfig = ConfigFactory.parseFile(new java.io.File(pipeLinePath))
  val kafkaConfig = new KafkaConfig(pipeLineConfig)

  implicit var system: ActorSystem = null

  override def beforeAll: Unit = {
    system = ActorSystem("PipeLineDSLSpec")
  }

  override def afterAll: Unit = {
    system.shutdown()
  }

  property("SinkTask should call the passed in closure") {
    val taskContext = MockUtil.mockTaskContext
    val conf = UserConfig.empty
    val data = Array[Datum](Datum("CPU", "total", 1.401257775E7))
    val expected = Message(data)
    var called = false
    val sinkClosure: SinkClosure[Array[Datum]] = (sinkInterface: HBaseSinkInterface, hbaseConsumer: HBaseConsumer) => {
      metrics: Array[Datum] => {
        val LOG: Logger = LogUtil.getLogger(metrics.getClass)
        LOG.info("writing-to-HBase")
        called = true
      }
    }
    val sinkOp = new SinkConsumer(pipeLineConfig, sinkClosure)
    val task = new SinkTask(Some(sinkOp), taskContext, conf)
    task.onStart(StartTime(0))
    task.onNext(expected)
    taskContext.output(expected)
    assert(called)
  }

  property("StreamApp should readFromKafka") {
    val app = new StreamApp("PipeLineDSL", system, UserConfig.empty)
    val producer = KafkaDSLUtil.createStream[String](app, 1, "", kafkaConfig).map{ message =>
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
    })()).writeToHBase(pipeLineConfig, (sinkInterface: HBaseSinkInterface, hbaseConsumer: HBaseConsumer) => {
      metrics: Array[Datum] => {
        val LOG: Logger = LogUtil.getLogger(metrics.getClass)
        LOG.info("writing-to-HBase")
      }
    })
    val graphVertices = List(
      "org.apache.gearpump.streaming.dsl.op.DataSourceOp",
      "org.apache.gearpump.streaming.dsl.op.FlatMapOp",
      "org.apache.gearpump.streaming.dsl.op.FlatMapOp",
      "org.apache.gearpump.streaming.dsl.op.ReduceOp",
      "org.apache.gearpump.streaming.dsl.op.TraversableSink"
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

