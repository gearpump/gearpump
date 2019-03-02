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
import io.gearpump.streaming.Processor
import io.gearpump.streaming.Processor.DefaultProcessor
import io.gearpump.streaming.dsl.plan.OpSpec._
import io.gearpump.streaming.dsl.plan.functions.{DummyRunner, FlatMapper, FunctionRunner}
import io.gearpump.streaming.dsl.scalaapi.functions.FlatMapFunction
import io.gearpump.streaming.dsl.window.api.GlobalWindows
import io.gearpump.streaming.dsl.window.impl.{StreamingOperator, WindowOperator}
import io.gearpump.streaming.sink.DataSink
import io.gearpump.streaming.source.DataSource
import io.gearpump.streaming.task.{Task, TaskContext}
import java.time.Instant
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.scalatest.mockito.MockitoSugar
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class OpSpec extends WordSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  private val unchainableOps: List[Op] = List(
    mock[DataSourceOp],
    mock[DataSinkOp],
    mock[GroupByOp[Any, Any]],
    mock[MergeOp],
    mock[ProcessorOp[AnyTask]])

  implicit var system: ActorSystem = _

  override def beforeAll(): Unit = {
    system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
  }

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }

  "DataSourceOp" should {

    "chain TransformOp" in {
      val dataSource = new AnySource
      val dataSourceOp = DataSourceOp(dataSource)
      val transformOp = mock[TransformOp[Any, Any]]
      val fn = mock[FunctionRunner[Any, Any]]
      when(transformOp.runner).thenReturn(fn)

      val chainedOp = dataSourceOp.chain(transformOp)

      chainedOp shouldBe a[DataSourceOp]

      unchainableOps.foreach { op =>
        intercept[OpChainException] {
          dataSourceOp.chain(op)
        }
      }
    }

    "be translated into processor" in {
      val dataSource = new AnySource
      val dataSourceOp = DataSourceOp(dataSource)
      val processor = dataSourceOp.toProcessor
      processor shouldBe a[Processor[_]]
      processor.parallelism shouldBe dataSourceOp.parallelism
      processor.description shouldBe s"${dataSourceOp.description}"
    }
  }

  "DataSinkOp" should {

    "not chain any Op" in {
      val dataSink = new AnySink
      val dataSinkOp = DataSinkOp(dataSink)
      val chainableOp = mock[TransformOp[Any, Any]]
      val ops = chainableOp +: unchainableOps
      ops.foreach { op =>
        intercept[OpChainException] {
          dataSinkOp.chain(op)
        }
      }
    }

    "be translated to processor" in {
      val dataSink = new AnySink
      val dataSinkOp = DataSinkOp(dataSink)
      val processor = dataSinkOp.toProcessor
      processor shouldBe a[Processor[_]]
      processor.parallelism shouldBe dataSinkOp.parallelism
      processor.description shouldBe dataSinkOp.description
    }
  }

  "ProcessorOp" should {

    "not chain any Op" in {
      val processorOp = new ProcessorOp[AnyTask]
      val chainableOp = mock[TransformOp[Any, Any]]
      val ops = chainableOp +: unchainableOps
      ops.foreach { op =>
        intercept[OpChainException] {
          processorOp.chain(op)
        }
      }
    }

    "be translated into processor" in {
      val processorOp = new ProcessorOp[AnyTask]
      val processor = processorOp.toProcessor
      processor shouldBe a [DefaultProcessor[_]]
      processor.parallelism shouldBe processorOp.parallelism
      processor.description shouldBe processorOp.description
    }
  }

  "TransformOp" should {

    "chain TransformOp" in {
      val fn1 = mock[FunctionRunner[Any, Any]]
      val transformOp1 = TransformOp[Any, Any](fn1)

      val fn2 = mock[FunctionRunner[Any, Any]]
      val transformOp2 = TransformOp[Any, Any](fn2)

      val chainedOp = transformOp1.chain(transformOp2)

      chainedOp shouldBe a[TransformOp[_, _]]

      unchainableOps.foreach { op =>
        intercept[OpChainException] {
          transformOp1.chain(op)
        }
      }
    }

    "be translated to processor" in {
      val fn = mock[FlatMapFunction[Any, Any]]
      val flatMapper = new FlatMapper(fn, "flatMap")
      val transformOp = TransformOp[Any, Any](flatMapper)

      val processor = transformOp.toProcessor
      processor shouldBe a[Processor[_]]
      processor.parallelism shouldBe 1
    }
  }

  "GroupByOp" should {

    val groupBy = (any: Any) => any
    val groupByOp = GroupByOp[Any, Any](groupBy)

    "chain WindowTransformOp" in {

      val runner = new WindowOperator[Any, Any](GlobalWindows(), new DummyRunner())
      val windowTransformOp = mock[WindowTransformOp[Any, Any]]
      when(windowTransformOp.operator).thenReturn(runner)

      val chainedOp = groupByOp.chain(windowTransformOp)
      chainedOp shouldBe a[GroupByOp[_, _]]

      unchainableOps.foreach { op =>
        intercept[OpChainException] {
          groupByOp.chain(op)
        }
      }
    }

    "be translated to processor" in {
      val processor = groupByOp.toProcessor
      processor shouldBe a[Processor[_]]
      processor.parallelism shouldBe 1
    }
  }

  "MergeOp" should {

    val mergeOp = MergeOp()

    "chain WindowTransformOp" in {
      val runner = mock[StreamingOperator[Any, Any]]
      val windowTransformOp = mock[WindowTransformOp[Any, Any]]
      when(windowTransformOp.operator).thenReturn(runner)

      val chainedOp = mergeOp.chain(windowTransformOp)
      chainedOp shouldBe a [MergeOp]

      unchainableOps.foreach { op =>
        intercept[OpChainException] {
          mergeOp.chain(op)
        }
      }
    }

    "be translated to processor" in {
      val processor = mergeOp.toProcessor
      processor shouldBe a[Processor[_]]
      processor.parallelism shouldBe 1
    }
  }
}

object OpSpec {
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
