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

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.gearpump._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.experiments.hbase.HBaseSink._
import org.apache.gearpump.experiments.hbase._
import org.apache.gearpump.experiments.pipeline.Messages._
import org.apache.gearpump.streaming.dsl.StreamApp
import org.apache.gearpump.streaming.dsl.StreamApp._
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.streaming.transaction.api.TimeReplayableSource
import org.apache.gearpump.util.{Constants, LogUtil}
import org.apache.hadoop.conf.Configuration
import org.slf4j.Logger
import upickle._

import scala.util.Try

class TimeReplayableSourceTest1 extends TimeReplayableSource {
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
object PipeLineDSL extends App with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "conf" -> CLIOption[String]("<conf file>", required = true)
  )
  val context = ClientContext()
  implicit val system = context.system
/*
  def toHBaseSink(taskContext: TaskContext, userConfig: UserConfig): Array[Datum] => Unit = {
    val repo = new HBaseRepo {
      def getHBase(table: String, conf: Configuration): HBaseSinkInterface = HBaseSink(table, conf)
    }
    val pipeLineConfig: PipeLineConfig = userConfig.getValue[PipeLineConfig](PIPELINE).get
    val hbaseConsumer = HBaseConsumer(taskContext.system, Some(pipeLineConfig.config))
    val hbase = hbaseConsumer.getHBase(repo)
    metrics: Array[Datum] => {
      val LOG: Logger = LogUtil.getLogger(metrics.getClass)
      LOG.info("writing-to-HBase")
      metrics.foreach(datum => {
        hbase.insert(System.currentTimeMillis.toString, hbaseConsumer.table, hbaseConsumer.table, datum.value.toString)
      })
    }
  }
*/
  def toHBaseSink(taskContext: TaskContext, userConfig: UserConfig): Array[Datum] => Unit = {
    val pipeLineConfig: PipeLineConfig = userConfig.getValue[PipeLineConfig](PIPELINE).get
    metrics: Array[Datum] => {
      val LOG: Logger = LogUtil.getLogger(metrics.getClass)
      LOG.info("writing-to-HBase")
    }
  }

  def application(context: ClientContext, config: ParseResult): Unit = {
    val pipeLinePath = config.getString("conf")
    val pipeLineConfig = ConfigFactory.parseFile(new java.io.File(pipeLinePath))
    val kafkaConfig = KafkaConfig(pipeLineConfig)
    val repo = new HBaseRepo {
      def getHBase(table: String, conf: Configuration): HBaseSinkInterface = HBaseSink(table, conf)
    }
    val appConfig = UserConfig.empty.withValue(KafkaConfig.NAME, kafkaConfig).withValue(PIPELINE, pipeLineConfig).withValue(HBASESINK, repo)
    System.setProperty(Constants.GEARPUMP_CUSTOM_CONFIG_FILE, pipeLinePath)

    val app = StreamApp("PipeLineDSL", context, appConfig)
/*
    val producer = app.readFromKafka(kafkaConfig, msg => {
      val jsonData = msg.msg.asInstanceOf[String]
      val envelope = read[Envelope](jsonData)
      val body = read[Body](envelope.body)
      body.metrics
    }, 1, "time-replayable-producer")

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
      val average = TAverage(pipeLineConfig.getInt(CPU_INTERVAL))
      msg: Array[Datum] => {
        val now = System.currentTimeMillis
        msg.flatMap(datum => {
          average.average(datum, now)
        })
      }
    })()).writeToHBase(((config:Config) => {
      (taskContext:TaskContext, userConfig:UserConfig) => {
        val repo = new HBaseRepo {
          def getHBase(table: String, conf: Configuration): HBaseSinkInterface = HBaseSink(table, conf)
        }
        val hbaseConsumer = HBaseConsumer(taskContext.system, Some(config))
        val hbase = hbaseConsumer.getHBase(repo)
        metrics: Array[Datum] => {
          val LOG: Logger = LogUtil.getLogger(metrics.getClass)
          LOG.info("writing-to-HBase")
          metrics.foreach(datum => {
            hbase.insert(System.currentTimeMillis.toString, hbaseConsumer.table, hbaseConsumer.table, datum.value.toString)
          })
        }
      }
    })(pipeLineConfig))

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
      val average = TAverage(pipeLineConfig.getInt(MEM_INTERVAL))
      msg: Array[Datum] => {
        val now = System.currentTimeMillis
        msg.flatMap(datum => {
          average.average(datum, now)
        })
      }
    })()).writeToHBase(((config:Config) => {
      (taskContext:TaskContext, userConfig:UserConfig) => {
        val repo = new HBaseRepo {
          def getHBase(table: String, conf: Configuration): HBaseSinkInterface = HBaseSink(table, conf)
        }
        val hbaseConsumer = HBaseConsumer(taskContext.system, Some(config))
        val hbase = hbaseConsumer.getHBase(repo)
        metrics: Array[Datum] => {
          val LOG: Logger = LogUtil.getLogger(metrics.getClass)
          LOG.info("writing-to-HBase")
          metrics.foreach(datum => {
            hbase.insert(System.currentTimeMillis.toString, hbaseConsumer.table, hbaseConsumer.table, datum.value.toString)
          })
        }
      }
    })(pipeLineConfig))
    */


val producer = app.readFromTimeReplayableSource(new TimeReplayableSourceTest1, msg => {
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
      val average = TAverage(pipeLineConfig.getInt(CPU_INTERVAL))
      msg: Array[Datum] => {
        val now = System.currentTimeMillis
        msg.flatMap(datum => {
          average.average(datum, now)
        })
      }
    })()).writeToHBase(((config:Config) => {
      val ZOOKEEPER = "hbase.zookeeper.connect"
      val TABLE_NAME = "hbase.table.name"
      val COLUMN_FAMILY = "hbase.table.column.family"
      val COLUMN_NAME = "hbase.table.column.name"
      (taskContext:TaskContext, userConfig:UserConfig) => {
        val LOG: Logger = LogUtil.getLogger(taskContext.getClass)
        val (zookeepers, (table, family, column)) = Some(config).map(config => {
          val zookeepers = config.getString(ZOOKEEPER)
          val table = config.getString(TABLE_NAME)
          val family = config.getString(COLUMN_FAMILY)
          val column = config.getString(COLUMN_NAME)
          (zookeepers, (table, family, column))
        }).get
        metrics: Array[Datum] => {
          LOG.info(s"writing-to-HBase ${zookeepers} ${table} ${family} ${column}")
        }
      }
    })(pipeLineConfig))

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
      val average = TAverage(pipeLineConfig.getInt(MEM_INTERVAL))
      msg: Array[Datum] => {
        val now = System.currentTimeMillis
        msg.flatMap(datum => {
          average.average(datum, now)
        })
      }
    })()).writeToHBase(((config:Config) => {
      val ZOOKEEPER = "hbase.zookeeper.connect"
      val TABLE_NAME = "hbase.table.name"
      val COLUMN_FAMILY = "hbase.table.column.family"
      val COLUMN_NAME = "hbase.table.column.name"
      (taskContext:TaskContext, userConfig:UserConfig) => {
        val LOG: Logger = LogUtil.getLogger(taskContext.getClass)
        val (zookeepers, (table, family, column)) = Some(config).map(config => {
          val zookeepers = config.getString(ZOOKEEPER)
          val table = config.getString(TABLE_NAME)
          val family = config.getString(COLUMN_FAMILY)
          val column = config.getString(COLUMN_NAME)
          (zookeepers, (table, family, column))
        }).get
        metrics: Array[Datum] => {
          LOG.info(s"writing-to-HBase ${zookeepers} ${table} ${family} ${column}")
        }
      }
    })(pipeLineConfig))

    context.submit(app)
    context.close()

  }
  Try({
    application(context, parse(args))
  }).failed.foreach(throwable => {
    LOG.error("Application Failed", throwable)
  })
}
