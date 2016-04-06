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

package io.gearpump.streaming.dsl

import scala.language.implicitConversions

import akka.actor.ActorSystem

import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.client.ClientContext
import io.gearpump.streaming.StreamApplication
import io.gearpump.streaming.dsl.op.{DataSourceOp, Op, OpEdge, ProcessorOp}
import io.gearpump.streaming.dsl.plan.Planner
import io.gearpump.streaming.source.DataSource
import io.gearpump.streaming.task.{Task, TaskContext}
import io.gearpump.util.Graph
import io.gearpump.{Message, TimeStamp}

/**
 * Example:
 * {{{
 * val data = "This is a good start, bingo!! bingo!!"
 * app.fromCollection(data.lines.toList).
 * // word => (word, count)
 * flatMap(line => line.split("[\\s]+")).map((_, 1)).
 * // (word, count1), (word, count2) => (word, count1 + count2)
 * groupBy(kv => kv._1).reduce(sum(_, _))
 *
 * val appId = context.submit(app)
 * context.close()
 * }}}
 *
 * @param name name of app
 */
class StreamApp(
    val name: String, system: ActorSystem, userConfig: UserConfig, val graph: Graph[Op, OpEdge]) {

  def this(name: String, system: ActorSystem, userConfig: UserConfig) = {
    this(name, system, userConfig, Graph.empty[Op, OpEdge])
  }

  def plan(): StreamApplication = {
    implicit val actorSystem = system
    val planner = new Planner
    val dag = planner.plan(graph)
    StreamApplication(name, dag, userConfig)
  }
}

object StreamApp {
  def apply(name: String, context: ClientContext, userConfig: UserConfig = UserConfig.empty)
    : StreamApp = {
    new StreamApp(name, context.system, userConfig)
  }

  implicit def streamAppToApplication(streamApp: StreamApp): StreamApplication = {
    streamApp.plan
  }

  implicit class Source(app: StreamApp) extends java.io.Serializable {

    def source[T](dataSource: DataSource, parallism: Int): Stream[T] = {
      source(dataSource, parallism, UserConfig.empty)
    }

    def source[T](dataSource: DataSource, parallism: Int, description: String): Stream[T] = {
      source(dataSource, parallism, UserConfig.empty, description)
    }

    def source[T](dataSource: DataSource, parallism: Int, conf: UserConfig): Stream[T] = {
      source(dataSource, parallism, conf, description = null)
    }

    def source[T](dataSource: DataSource, parallism: Int, conf: UserConfig, description: String)
      : Stream[T] = {
      implicit val sourceOp = DataSourceOp(dataSource, parallism, conf, description)
      app.graph.addVertex(sourceOp)
      new Stream[T](app.graph, sourceOp)
    }
    def source[T](seq: Seq[T], parallism: Int, description: String): Stream[T] = {
      this.source(new CollectionDataSource[T](seq), parallism, UserConfig.empty, description)
    }

    def source[T](source: Class[_ <: Task], parallism: Int, conf: UserConfig, description: String)
      : Stream[T] = {
      val sourceOp = ProcessorOp(source, parallism, conf, Option(description).getOrElse("source"))
      app.graph.addVertex(sourceOp)
      new Stream[T](app.graph, sourceOp)
    }
  }
}

class CollectionDataSource[T](seq: Seq[T]) extends DataSource {
  val list = seq.toList
  var index = 0

  def readOne(): List[Message] = {
    if (index < list.length) {
      val element = List(Message(list(index).asInstanceOf[AnyRef]))
      index += 1
      element
    } else {
      List.empty[Message]
    }
  }

  override def read(batchSize: Int): List[Message] = {
    readOne()
  }

  override def close(): Unit = {}

  override def open(context: TaskContext, startTime: Option[TimeStamp]): Unit = {}
}