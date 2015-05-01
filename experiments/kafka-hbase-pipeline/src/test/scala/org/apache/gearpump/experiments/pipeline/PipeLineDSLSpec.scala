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
import org.apache.gearpump._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.experiments.hbase.HBaseSinkInterface
import org.apache.gearpump.experiments.pipeline.Messages._
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.dsl.StreamApp
import org.apache.gearpump.streaming.dsl.op.OpType._
import org.apache.gearpump.streaming.dsl.op.SinkOp
import org.apache.gearpump.streaming.dsl.plan.OpTranslator.SinkTask
import org.apache.gearpump.streaming.kafka.KafkaSource
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.task.{StartTime, TaskContext, TaskId}
import org.apache.gearpump.streaming.transaction.api.{MessageDecoder, TimeReplayableSource}
import org.apache.gearpump.util.{Constants, LogUtil}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}
import org.slf4j.Logger
import upickle._

class TimeReplayableSourceTest extends TimeReplayableSource {
  val data = Array[String](
    """
      |{"id":"2a329674-12ad-49f7-b40d-6485aae0aae8","on":"2015-04-02T18:52:02.680178753Z","body":"{\"sample_id\":\"sample-0\",\"source_id\":\"src-13\",\"event_ts\":\"2015-04-02T18:52:04.784086993Z\",\"metrics\":[{\"dimension\":\"CPU\",\"metric\":\"total\",\"value\":27993997},{\"dimension\":\"CPU\",\"metric\":\"user\",\"value\":39018},{\"dimension\":\"CPU\",\"metric\":\"sys\",\"value\":23299},{\"dimension\":\"LOAD\",\"metric\":\"min\",\"value\":0},{\"dimension\":\"MEM\",\"metric\":\"free\",\"value\":15607009280},{\"dimension\":\"MEM\",\"metric\":\"used\",\"value\":163528704}]}"}
    """
      .stripMargin,
    """
      |{"id":"043ade58-2fbc-4fe2-8253-84ab181b8cfa","on":"2015-04-02T18:52:02.680078434Z","body":"{\"sample_id\":\"sample-0\",\"source_id\":\"src-4\",\"event_ts\":\"2015-04-02T18:52:04.78364878Z\",\"metrics\":[{\"dimension\":\"CPU\",\"metric\":\"total\",\"value\":27993996},{\"dimension\":\"CPU\",\"metric\":\"user\",\"value\":39017},{\"dimension\":\"CPU\",\"metric\":\"sys\",\"value\":23299},{\"dimension\":\"LOAD\",\"metric\":\"min\",\"value\":0},{\"dimension\":\"MEM\",\"metric\":\"free\",\"value\":15607009280},{\"dimension\":\"MEM\",\"metric\":\"used\",\"value\":163528704}]}"}
    """.stripMargin,
    """
      |{"id":"043ade58-2fbc-4fe2-8253-84ab181b8cfa","on":"2015-04-02T18:52:02.680078434Z","body":"{\"sample_id\":\"sample-0\",\"source_id\":\"src-4\",\"event_ts\":\"2015-04-02T18:52:04.78364878Z\",\"metrics\":[{\"dimension\":\"CPU\",\"metric\":\"total\",\"value\":27993996},{\"dimension\":\"CPU\",\"metric\":\"user\",\"value\":39017},{\"dimension\":\"CPU\",\"metric\":\"sys\",\"value\":23299},{\"dimension\":\"LOAD\",\"metric\":\"min\",\"value\":0},{\"dimension\":\"MEM\",\"metric\":\"free\",\"value\":15607009280},{\"dimension\":\"MEM\",\"metric\":\"used\",\"value\":163528704}]}"}
    """.stripMargin
  )

  def startFromBeginning(): Unit = {}

  def setStartTime(startTime: TimeStamp): Unit = {}

  def pull(num: Int): List[Message] = List(Message(data(0)), Message(data(1)), Message(data(2)))

  def close(): Unit = {}
}

class KafkaSourceTest(kafkaConfig: KafkaConfig) extends Traverse[Array[Datum]] {
  private val batchSize = kafkaConfig.getConsumerEmitBatchSize
  private val msgDecoder: MessageDecoder = kafkaConfig.getMessageDecoder
  lazy val source = new KafkaSource(kafkaConfig.getClientId, TaskId(0,0), 1, kafkaConfig, msgDecoder)
  override def foreach[U](fun: Array[Datum] => U): Unit = {
    val list = source.pull(10)
    list.foreach(msg => {
      val jsonData = msg.msg.asInstanceOf[String]
      val envelope = read[Envelope](jsonData)
      val body = read[Body](envelope.body)
      val metrics = body.metrics
      fun(metrics)
    })
  }
}

class PipeLineDSLSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter {
  val LOG: Logger = LogUtil.getLogger(getClass)
  val PROCESSORS = "pipeline.processors"
  val PERSISTORS = "pipeline.persistors"
  val pipeLinePath = "conf/pipeline.conf.template"
  val pipeLineConfig = PipeLineConfig(ConfigFactory.parseFile(new java.io.File(pipeLinePath)))

  property("StreamApp should allow UserConfig and ClusterConfig") {
    val persistors = pipeLineConfig.config.getInt(PERSISTORS)
    val kafkaConfig = KafkaConfig(pipeLineConfig.config)
    val context = ClientContext()
    implicit val system = context.system
    val appConfig = UserConfig.empty.withValue(KafkaConfig.NAME, kafkaConfig).withValue(PIPELINE, pipeLineConfig)
    System.setProperty(Constants.GEARPUMP_CUSTOM_CONFIG_FILE, pipeLinePath)

  }
  property("StreamApp should readFromTimeReplayableSource") {
    val context = ClientContext()
    val app = StreamApp("PipeLineDSL", context)
    val producer = app.readFromTimeReplayableSource(new TimeReplayableSourceTest, msg => {
      val jsonData = msg.msg.asInstanceOf[String]
      val envelope = read[Envelope](jsonData)
      val body = read[Body](envelope.body)
      body.metrics
    }, 10, 1, "time-replayable-producer").flatMap(metrics => {
      Some(metrics.flatMap(datum => {
        datum.dimension match {
          case CPU =>
            Some(datum)
          case _ =>
            None
        }
      }))
    }).reduce((() => {
      val average = TAverage(10)
      (msg1: Array[Datum], msg2: Array[Datum]) => {
        val now = System.currentTimeMillis
        msg2.flatMap(datum => {
          average.average(datum, now)
        })
      }
    })()).sink((taskContext, userConfig) => {
      val LOG: Logger = LogUtil.getLogger(taskContext.getClass)
      val hbase = new HBaseSinkInterface {
        def insert(rowKey: Array[Byte], columnGroup: Array[Byte], columnName: Array[Byte], value: Array[Byte]): Unit = {}
        def insert(rowKey: String, columnGroup: String, columnName: String, value: String): Unit = {
          LOG.info(s"value=$value")
        }
        def close(): Unit = {}
      }
      def hbaseSink(hbase: HBaseSinkInterface)(metrics: Array[Datum]): Unit = {
        metrics.foreach(datum => {
          hbase.insert(System.currentTimeMillis.toString, "pipeline", "pipeline", datum.value.toString)
        })
      }
      hbaseSink(hbase)
    })
    val graphVertices = List(
      "org.apache.gearpump.streaming.dsl.op.TraversableSource",
      "org.apache.gearpump.streaming.dsl.op.FlatMapOp",
      "org.apache.gearpump.streaming.dsl.op.ReduceOp",
      "org.apache.gearpump.streaming.dsl.op.SinkOp"
    )
    var i = 0
    app.graph.vertices.foreach(op => {
      assert(graphVertices(i) == op.getClass.getName)
      i = i+1
    })
    app.plan.dag.vertices.foreach(desc => {
      LOG.info(s"taskClass=${desc.taskClass}")
    })
  }
  property("SinkTask should call the passed in closure") {
    val taskContext = MockUtil.mockTaskContext
    val conf = UserConfig.empty
    val data = Array[Datum](Datum("CPU", "total", 1.401257775E7))
    val expected = Message(data)
    var called = false
    val closure: Closure[Array[Datum]] = (taskContext: TaskContext, userConfig: UserConfig) => {
      metrics: Array[Datum] => {
        called = true
      }
    }
    val sinkOp = SinkOp(closure, 1, "sink")
    val task = new SinkTask(Some(sinkOp), taskContext, conf)
    task.onStart(StartTime(0))
    task.onNext(expected)
    taskContext.output(expected)
    assert(called)

  }
  property("StreamApp should readFromReplayableSource -> flatMap -> map -> sink") {
    val context = ClientContext()
    val app = StreamApp("PipeLineDSL", context)
    val producer = app.readFromTimeReplayableSource(new TimeReplayableSourceTest, msg => {
      val jsonData = msg.msg.asInstanceOf[String]
      val envelope = read[Envelope](jsonData)
      val body = read[Body](envelope.body)
      body.metrics
    }, 10, 1, "time-replayable-producer")

    producer.flatMap(metrics => {
      Some(metrics.flatMap(datum => {
        datum.dimension match {
          case CPU =>
            Some(datum)
          case _ =>
            None
        }
      }))
    }).map((() => {
      val average = TAverage(pipeLineConfig.config.getInt(CPU_INTERVAL))
      val LOG: Logger = LogUtil.getLogger(average.getClass)
      msg: Array[Datum] => {
        val now = System.currentTimeMillis
        msg.flatMap(datum => {
          val data = average.average(datum, now)
          data match {
            case Some(d) =>
            case None =>
          }
          data
        })
      }
    })()).sink((taskContext, userConfig) => {
      metrics: Array[Datum] => {
        val LOG: Logger = LogUtil.getLogger(metrics.getClass)
        LOG.info("CPU Point 1")
      }
    })

    producer.flatMap(metrics => {
      Some(metrics.flatMap(datum => {
        datum.dimension match {
          case MEM =>
            Some(datum)
          case _ =>
            None
        }
      }))
    }).map((() => {
      val average = TAverage(pipeLineConfig.config.getInt(MEM_INTERVAL))
      val LOG: Logger = LogUtil.getLogger(average.getClass)
      msg: Array[Datum] => {
        val now = System.currentTimeMillis
        msg.flatMap(datum => {
          val data = average.average(datum, now)
          data match {
            case Some(d) =>
              LOG.info("valid data")
            case None =>
          }
          data
        })
      }
    })()).sink((taskContext, userConfig) => {
      metrics: Array[Datum] => {
        val LOG: Logger = LogUtil.getLogger(metrics.getClass)
        LOG.info("MEM Point 1")
      }
    })
  }
  property("StreamApp should readFromKafka") {
    val pipeLinePath = "conf/pipeline.conf.template"
    val pipelineConfig = PipeLineConfig(ConfigFactory.parseFile(new java.io.File(pipeLinePath)))
    val kafkaConfig = KafkaConfig(pipelineConfig.config)

    val context = ClientContext()
    val app = StreamApp("PipeLineDSL", context)
    val producer = app.readFromKafka(kafkaConfig, msg => {
      val jsonData = msg.msg.asInstanceOf[String]
      val envelope = read[Envelope](jsonData)
      val body = read[Body](envelope.body)
      body.metrics
    }, 1, "time-replayable-producer").flatMap(metrics => {
      Some(metrics.flatMap(datum => {
        datum.dimension match {
          case CPU =>
            Some(datum)
          case _ =>
            None
        }
      }))
    }).reduce((() => {
      val average = TAverage(pipelineConfig.config.getInt(CPU_INTERVAL))
      (msg1: Array[Datum], msg2: Array[Datum]) => {
        val now = System.currentTimeMillis
        msg2.flatMap(datum => {
          average.average(datum, now)
        })
      }
    })()).sink((taskContext, userConfig) => {
      val LOG: Logger = LogUtil.getLogger(taskContext.getClass)
      val hbase = new HBaseSinkInterface {
        def insert(rowKey: Array[Byte], columnGroup: Array[Byte], columnName: Array[Byte], value: Array[Byte]): Unit = {}
        def insert(rowKey: String, columnGroup: String, columnName: String, value: String): Unit = {
          LOG.info(s"value=$value")
        }
        def close(): Unit = {}
      }
      def hbaseSink(hbase: HBaseSinkInterface)(metrics: Array[Datum]): Unit = {
        metrics.foreach(datum => {
          hbase.insert(System.currentTimeMillis.toString, "pipeline", "pipeline", datum.value.toString)
        })
      }
      hbaseSink(hbase)
    })
    val graphVertices = List(
      "org.apache.gearpump.streaming.dsl.op.TraversableSource",
      "org.apache.gearpump.streaming.dsl.op.FlatMapOp",
      "org.apache.gearpump.streaming.dsl.op.ReduceOp",
      "org.apache.gearpump.streaming.dsl.op.SinkOp"
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

