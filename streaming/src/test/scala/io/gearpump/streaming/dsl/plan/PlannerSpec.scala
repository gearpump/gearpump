/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.streaming.dsl.plan

import akka.actor.ActorSystem
import io.gearpump.Message
import io.gearpump.cluster.{TestUtil, UserConfig}
import io.gearpump.streaming.MockUtil
import io.gearpump.streaming.dsl.api.functions.ReduceFunction
import io.gearpump.streaming.dsl.plan.PlannerSpec._
import io.gearpump.streaming.dsl.plan.functions.{FlatMapper, FoldRunner}
import io.gearpump.streaming.dsl.scalaapi.functions.FlatMapFunction
import io.gearpump.streaming.dsl.window.api.GlobalWindows
import io.gearpump.streaming.partitioner.{CoLocationPartitioner, GroupByPartitioner}
import io.gearpump.streaming.sink.DataSink
import io.gearpump.streaming.source.DataSource
import io.gearpump.streaming.task.{Task, TaskContext}
import io.gearpump.util.Graph
import java.time.Instant
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatest.mockito.MockitoSugar
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
    graph.addVertexAndEdge(sourceOp, shuffleEdge, groupByOp)
    graph.addVertex(windowOp)
    graph.addVertexAndEdge(groupByOp, directEdge, windowOp)
    graph.addVertex(flatMapOp)
    graph.addVertexAndEdge(windowOp, directEdge, flatMapOp)
    graph.addVertex(reduceOp)
    graph.addVertexAndEdge(flatMapOp, directEdge, reduceOp)
    graph.addVertex(processorOp)
    graph.addVertexAndEdge(reduceOp, directEdge, processorOp)
    graph.addVertex(sinkOp)
    graph.addVertexAndEdge(processorOp, directEdge, sinkOp)

    implicit val system = MockUtil.system

    val planner = new Planner
    val plan = planner.plan(graph)
      .mapVertex(_.description)

    plan.getVertices.toSet should contain theSameElementsAs
      Set("source", "groupBy.globalWindows.flatMap.reduce", "processor", "sink")
    plan.outgoingEdgesOf("source").iterator.next()._2 shouldBe
      a[GroupByPartitioner[_, _]]
    plan.outgoingEdgesOf("groupBy.globalWindows.flatMap.reduce").iterator.next()._2 shouldBe
      a[CoLocationPartitioner]
    plan.outgoingEdgesOf("processor").iterator.next()._2 shouldBe a[CoLocationPartitioner]
  }
}

object PlannerSpec {

  private val anyFlatMapper = new FlatMapper[Any, Any](
    FlatMapFunction((t => Option(t)): Any => TraversableOnce[Any]), "flatMap")
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
