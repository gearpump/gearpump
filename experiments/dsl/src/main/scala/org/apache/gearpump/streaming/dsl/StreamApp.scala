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

import akka.actor.{ActorRef, ActorSystem, Cancellable, Props}
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.streaming.StreamApplication
import org.apache.gearpump.streaming.dsl.op.OpType.{Traverse, TraverseType}
import org.apache.gearpump.streaming.dsl.op._
import org.apache.gearpump.streaming.dsl.plan.Planner
import org.apache.gearpump.streaming.kafka.KafkaSource
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.task.{TaskContext, TaskId}
import org.apache.gearpump.streaming.transaction.api.TimeReplayableSource
import org.apache.gearpump.util.Graph
import org.apache.gearpump.{Message, TimeStamp}

import scala.concurrent.duration.FiniteDuration
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
class StreamApp(val name: String, system: ActorSystem, userConfig: UserConfig) {

  val graph = Graph.empty[Op, OpEdge]

  def plan(): StreamApplication = {
    implicit val actorSystem = system
    val planner = new Planner
    val dag = planner.plan(graph)
    StreamApplication(name, dag, userConfig)
  }
}

object StreamApp {
  def apply(name: String, context: ClientContext, userConfig: UserConfig = UserConfig.empty) = new StreamApp(name, context.system, userConfig)

  implicit def streamAppToApplication(streamApp: StreamApp): StreamApplication = {
    streamApp.plan
  }

  implicit class Source(app: StreamApp) extends java.io.Serializable {
    def source[M[_] <: TraverseType[_], T: ClassTag](traversable: M[T], parallism: Int, description: String = null): Stream[T] = {
      implicit val source = TraversableSource(traversable, parallism, Some(description).getOrElse("traversable"))
      app.graph.addVertex(source)
      new Stream[T](app.graph, source)
    }
    def readFromTimeReplayableSource[T: ClassTag](timeReplayableSource: TimeReplayableSource, converter: Message => T, batchSize: Int, parallism: Int, description: String = null): Stream[T] = {
      this.source(new TimeReplayableProducer(timeReplayableSource, converter, batchSize), parallism, description)
    }
    def readFromKafka[T: ClassTag](kafkaConfig: KafkaConfig, converter: Message => T, parallism: Int, description: String = null): Stream[T] = {
      this.source(new KafkaProducer(kafkaConfig, converter), parallism, description)
    }
  }
}

class TimeReplayableProducer[T:ClassTag](timeReplayableSource: TimeReplayableSource, converter: Message => T, batchSize: Int) extends Traverse[T] {
  val source = timeReplayableSource
  override def foreach[U](fun: T => U): Unit = {
    val list = source.read(batchSize)
    list.foreach(msg => {
      val metrics = converter(msg)
      fun(metrics)
    })
  }
}

class KafkaProducer[T:ClassTag](kafkaConfig: KafkaConfig, converter: Message => T) extends Traverse[T] {
  lazy val context = new TaskContext {
      override def taskId: TaskId = TaskId(0, 0)
      override def appName: String = "gearpump"
      override def schedule(initialDelay: FiniteDuration, interval: FiniteDuration)(f: => Unit): Cancellable = null
      override def appId: Int = 0
      override def executorId: Int = 0
      override def parallelism: Int = 1
      override def scheduleOnce(initialDelay: FiniteDuration)(f: => Unit): Cancellable = null
      override def self: ActorRef = null
      override def output(msg: Message): Unit = {}
      override def upstreamMinClock: TimeStamp = Message.noTimeStamp
      override def actorOf(props: Props): ActorRef = null
      override def actorOf(props: Props, name: String): ActorRef = null
      override def sender: ActorRef = null
      override def appMaster: ActorRef = null
      override def system: ActorSystem = null
    }
  lazy val source = Some(new KafkaSource(kafkaConfig)).map(kafkaSource => {
    kafkaSource.open(context, None)
    kafkaSource
  }).get

  lazy val batchSize = 100

  override def foreach[U](fun: T => U): Unit = {
    val list = source.read(batchSize)
    list.foreach(msg => {
      val metrics = converter(msg)
      fun(metrics)
    })
  }
}

