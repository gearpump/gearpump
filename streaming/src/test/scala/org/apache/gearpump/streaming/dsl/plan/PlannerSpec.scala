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

package org.apache.gearpump.streaming.dsl.plan

import java.time.Instant

import akka.actor.ActorSystem
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.{TestUtil, UserConfig}
import org.apache.gearpump.streaming.dsl.api.functions.ReduceFunction
import org.apache.gearpump.streaming.partitioner.{CoLocationPartitioner, GroupByPartitioner}
import org.apache.gearpump.streaming.dsl.plan.PlannerSpec._
import org.apache.gearpump.streaming.dsl.plan.functions.{FlatMapper, FoldRunner}
import org.apache.gearpump.streaming.dsl.scalaapi.functions.FlatMapFunction
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.source.DataSource
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.dsl.window.api.GlobalWindows
import org.apache.gearpump.streaming.task.{Task, TaskContext}
import org.apache.gearpump.util.Graph
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class PlannerSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  implicit var system: ActorSystem = _

  override def beforeAll(): Unit = {
    system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
  }

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }

  "Planner" should "chain operations" in {
    val graph = Graph.empty[Op, OpEdge]
    val sourceOp = DataSourceOp(new AnySource)
    val groupBy = (any: Any) => any
    val groupByOp = GroupByOp(groupBy)
    val windowOp = WindowOp(GlobalWindows())
    val flatMapOp = TransformOp[Any, Any](anyFlatMapper)
    val reduceOp = TransformOp[Any, Option[Any]](anyReducer)
    val processorOp = new ProcessorOp[AnyTask]
    val sinkOp = DataSinkOp(new AnySink)
    val directEdge = Direct
    val shuffleEdge = Shuffle

    graph.addVertex(sourceOp)
    graph.addVertex(groupByOp)
    graph.addEdge(sourceOp, shuffleEdge, groupByOp)
    graph.addVertex(windowOp)
    graph.addEdge(groupByOp, directEdge, windowOp)
    graph.addVertex(flatMapOp)
    graph.addEdge(windowOp, directEdge, flatMapOp)
    graph.addVertex(reduceOp)
    graph.addEdge(flatMapOp, directEdge, reduceOp)
    graph.addVertex(processorOp)
    graph.addEdge(reduceOp, directEdge, processorOp)
    graph.addVertex(sinkOp)
    graph.addEdge(processorOp, directEdge, sinkOp)

    implicit val system = MockUtil.system

    val planner = new Planner
    val plan = planner.plan(graph)
      .mapVertex(_.description)

    plan.getVertices.toSet should contain theSameElementsAs
      Set("source.globalWindows", "groupBy.globalWindows.flatMap.reduce", "processor", "sink")
    plan.outgoingEdgesOf("source.globalWindows").iterator.next()._2 shouldBe
      a[GroupByPartitioner[_, _]]
    plan.outgoingEdgesOf("groupBy.globalWindows.flatMap.reduce").iterator.next()._2 shouldBe
      a[CoLocationPartitioner]
    plan.outgoingEdgesOf("processor").iterator.next()._2 shouldBe a[CoLocationPartitioner]
  }
}

object PlannerSpec {

  private val anyFlatMapper = new FlatMapper[Any, Any](
    FlatMapFunction(Option(_)), "flatMap")
  private val anyReducer = new FoldRunner[Any, Option[Any]](
    ReduceFunction((left: Any, right: Any) => (left, right)), "reduce")

  class AnyTask(context: TaskContext, config: UserConfig) extends Task(context, config)

  class AnySource extends DataSource {

    override def open(context: TaskContext, startTime: Instant): Unit = {}

    override def read(): Message = Message("any")

    override def close(): Unit = {}

    override def getWatermark: Instant = Instant.now()
  }

  class AnySink extends DataSink {

    override def open(context: TaskContext): Unit = {}

    override def write(message: Message): Unit = {}

    override def close(): Unit = {}
  }
}
