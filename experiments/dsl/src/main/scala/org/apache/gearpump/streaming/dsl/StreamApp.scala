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

package org.apache.gearpump.streaming.dsl

import akka.actor.ActorSystem
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.streaming.StreamApplication
import org.apache.gearpump.streaming.dsl.op.OpType.{Traverse, TraverseType}
import org.apache.gearpump.streaming.dsl.op._
import org.apache.gearpump.streaming.dsl.plan.Planner
import org.apache.gearpump.streaming.kafka.KafkaSource
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.streaming.transaction.api.{MessageDecoder, TimeReplayableSource}
import org.apache.gearpump.util.Graph

import scala.reflect.ClassTag

/**
 * Example:
 *
 *
    val data = "This is a good start, bingo!! bingo!!"
    app.fromCollection(data.lines.toList).
      // word => (word, count)
      flatMap(line => line.split("[\\s]+")).map((_, 1)).
      // (word, count1), (word, count2) => (word, count1 + count2)
      groupBy(kv => kv._1).reduce(sum(_, _))

    val appId = context.submit(app)
    context.close()
 *
 * @param name name of app
 */
class StreamApp(val name: String, context: ClientContext, userConfig: UserConfig) {
  val graph = Graph.empty[Op, OpEdge]

  def plan(): StreamApplication = {
    implicit val system = context.system
    val planner = new Planner
    val dag = planner.plan(graph)
    StreamApplication(name, dag, userConfig)
  }

  private[StreamApp] def system: ActorSystem = context.system
}

object StreamApp {
  def apply(name: String, context: ClientContext, userConfig: UserConfig = UserConfig.empty) = new StreamApp(name, context, userConfig)

  implicit def streamAppToApplication(streamApp: StreamApp): StreamApplication = {
    streamApp.plan
  }

  implicit class Source(app: StreamApp) extends java.io.Serializable {
    def source[M[_] <: TraverseType[_], T: ClassTag](traversable: M[T], parallelism: Int, description: String = null): Stream[T] = {
      implicit val source = TraversableSource(traversable, parallelism, Some(description).getOrElse("traversable"))
      app.graph.addVertex(source)
      new Stream[T](app.graph, source)
    }
    def readFromTimeReplayableSource[T: ClassTag](timeReplayableSource: TimeReplayableSource, converter: Message => T, batchSize: Int, parallelism: Int, description: String = null): Stream[T] = {
      this.source(new TimeReplayableProducer(timeReplayableSource, converter, batchSize), parallelism, description)
    }
    def readFromKafka[T: ClassTag](kafkaConfig: KafkaConfig, converter: Message => T, parallelism: Int, description: String = null): Stream[T] = {
      this.source(new KafkaProducer(kafkaConfig, converter), parallelism, description)
    }
  }
}

class TimeReplayableProducer[T:ClassTag](timeReplayableSource: TimeReplayableSource, converter: Message => T, batchSize: Int) extends Traverse[T] {
  val source = timeReplayableSource
  override def foreach[U](fun: T => U): Unit = {
    val list = source.pull(batchSize)
    list.foreach(msg => {
      val metrics = converter(msg)
      fun(metrics)
    })
  }
}

class KafkaProducer[T:ClassTag](kafkaConfig: KafkaConfig, converter: Message => T) extends Traverse[T] {
  val batchSize = kafkaConfig.getConsumerEmitBatchSize
  val msgDecoder: MessageDecoder = kafkaConfig.getMessageDecoder
  lazy val source = Some(new KafkaSource(kafkaConfig.getClientId, TaskId(0, 0), 1, kafkaConfig, msgDecoder)).map(kafkaSource => {
    kafkaSource.startFromBeginning
    kafkaSource
  }).get

  override def foreach[U](fun: T => U): Unit = {
    val list = source.pull(batchSize)
    list.foreach(msg => {
      val metrics = converter(msg)
      fun(metrics)
    })
  }
}

