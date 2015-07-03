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
package org.apache.gearpump.examples.streaming.pipeline

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.apache.gearpump._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}
import org.apache.gearpump.streaming.transaction.api.TimeReplayableSource
import org.apache.gearpump.util.LogUtil
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfterAll, Matchers, PropSpec}
import org.slf4j.Logger

import scala.util.{Failure, Success, Try}

class SpaceShuttleReplayableSource extends TimeReplayableSource {
  val data = Array[String](
    """
      |{"id":"2a329674-12ad-49f7-b40d-6485aae0aae8","on":"2015-04-02T18:52:02.680178753Z","body":"[-0.414141,-0.0246564,-0.125,0.0140301,-0.474359,0.0256049,-0.0980392,0.463884,0.40836]"}
    """
      .stripMargin,
    """
      |{"id":"043ade58-2fbc-4fe2-8253-84ab181b8cfa","on":"2015-04-02T18:52:02.680078434Z","body": "[-0.414141,-0.0246564,-0.125,0.0140301,-0.474359,0.0256049,-0.0980392,0.463884,0.40836]"}
    """.stripMargin,
    """
      |{"id":"043ade58-2fbc-4fe2-8253-84ab181b8cfa","on":"2015-04-02T18:52:02.680078434Z","body": "[-0.414141,-0.0246564,-0.125,0.0140301,-0.474359,0.0256049,-0.0980392,0.463884,0.40836]"}
    """.stripMargin
  )

  override def open(context: TaskContext, startTime: Option[TimeStamp]): Unit = {}

  override def read(num: Int): List[Message] = List(Message(data(0)), Message(data(1)), Message(data(2)))

  override def close(): Unit = {}
}

class SpaceShuttleProducer(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {

  import taskContext.{output, parallelism}

  private val batchSize = 3

  val taskParallelism = parallelism

  private val source: TimeReplayableSource = new SpaceShuttleReplayableSource()
  private var startTime: TimeStamp = 0L

  override def onStart(newStartTime: StartTime): Unit = {
    startTime = newStartTime.startTime
    LOG.info(s"start time $startTime")
    source.open(taskContext, Some(startTime))
    self ! Message("start", System.currentTimeMillis())
  }

  override def onNext(msg: Message): Unit = {
    Try({

      source.read(batchSize).foreach(msg => {
        output(msg)
      })
    }) match {
      case Success(ok) =>
      case Failure(throwable) =>
        LOG.error(s"failed ${throwable.getMessage}")
    }
    self ! Message("continue", System.currentTimeMillis())
  }

  override def onStop(): Unit = {
    LOG.info("closing kafka source...")
    source.close()
  }
}

class KafkaHdfsPipeLineSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfterAll {
  val LOG: Logger = LogUtil.getLogger(getClass)
  implicit var system: ActorSystem = null

  override def beforeAll(): Unit = {
    system = ActorSystem("KafkaHdfsPipeLineSpec")
  }

  override def afterAll(): Unit = {
    system.shutdown()
  }

  property("KafkaHdfsPipeLineSpec should be able to create a DataSource") {
    Option(new SpaceShuttleReplayableSource) match {
      case Some(replayableSource) =>
      case None =>
        assert(false)
    }
  }
}

