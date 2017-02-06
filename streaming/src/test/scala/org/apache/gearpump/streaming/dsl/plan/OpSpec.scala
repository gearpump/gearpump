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
import org.apache.gearpump.streaming.Processor
import org.apache.gearpump.streaming.Processor.DefaultProcessor
import org.apache.gearpump.streaming.dsl.plan.OpSpec.{AnySink, AnySource, AnyTask}
import org.apache.gearpump.streaming.dsl.plan.functions.{FlatMapper, FunctionRunner}
import org.apache.gearpump.streaming.dsl.scalaapi.functions.FlatMapFunction
import org.apache.gearpump.streaming.dsl.window.api.GroupByFn
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.source.DataSource
import org.apache.gearpump.streaming.task.{Task, TaskContext}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.scalatest.mock.MockitoSugar

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

    "chain ChainableOp" in {
      val dataSource = new AnySource
      val dataSourceOp = DataSourceOp(dataSource)
      val chainableOp = mock[ChainableOp[Any, Any]]
      val fn = mock[FunctionRunner[Any, Any]]

      val chainedOp = dataSourceOp.chain(chainableOp)

      chainedOp shouldBe a[DataSourceOp]
      verify(chainableOp).fn

      unchainableOps.foreach { op =>
        intercept[OpChainException] {
          dataSourceOp.chain(op)
        }
      }
    }

    "get Processor of DataSource" in {
      val dataSource = new AnySource
      val dataSourceOp = DataSourceOp(dataSource)
      val processor = dataSourceOp.getProcessor
      processor shouldBe a[Processor[_]]
      processor.parallelism shouldBe dataSourceOp.parallelism
      processor.description shouldBe dataSourceOp.description
    }
  }

  "DataSinkOp" should {

    "not chain any Op" in {
      val dataSink = new AnySink
      val dataSinkOp = DataSinkOp(dataSink)
      val chainableOp = mock[ChainableOp[Any, Any]]
      val ops = chainableOp +: unchainableOps
      ops.foreach { op =>
        intercept[OpChainException] {
          dataSinkOp.chain(op)
        }
      }
    }

    "get Processor of DataSink" in {
      val dataSink = new AnySink
      val dataSinkOp = DataSinkOp(dataSink)
      val processor = dataSinkOp.getProcessor
      processor shouldBe a[Processor[_]]
      processor.parallelism shouldBe dataSinkOp.parallelism
      processor.description shouldBe dataSinkOp.description
    }
  }

  "ProcessorOp" should {

    "not chain any Op" in {
      val processorOp = new ProcessorOp[AnyTask]
      val chainableOp = mock[ChainableOp[Any, Any]]
      val ops = chainableOp +: unchainableOps
      ops.foreach { op =>
        intercept[OpChainException] {
          processorOp.chain(op)
        }
      }
    }

    "get Processor" in {
      val processorOp = new ProcessorOp[AnyTask]
      val processor = processorOp.getProcessor
      processor shouldBe a [DefaultProcessor[_]]
      processor.parallelism shouldBe processorOp.parallelism
      processor.description shouldBe processorOp.description
    }
  }

  "ChainableOp" should {

    "chain ChainableOp" in {
      val fn1 = mock[FunctionRunner[Any, Any]]
      val chainableOp1 = ChainableOp[Any, Any](fn1)

      val fn2 = mock[FunctionRunner[Any, Any]]
      val chainableOp2 = ChainableOp[Any, Any](fn2)

      val chainedOp = chainableOp1.chain(chainableOp2)

      chainedOp shouldBe a[ChainableOp[_, _]]

      unchainableOps.foreach { op =>
        intercept[OpChainException] {
          chainableOp1.chain(op)
        }
      }
    }

    "get Processor" in {
      val fn = mock[FlatMapFunction[Any, Any]]
      val flatMapper = new FlatMapper(fn, "flatMap")
      val chainableOp = ChainableOp[Any, Any](flatMapper)

      val processor = chainableOp.getProcessor
      processor shouldBe a[Processor[_]]
      processor.parallelism shouldBe 1
    }
  }

  "GroupByOp" should {

    "chain ChainableOp" in {
      val groupByFn = mock[GroupByFn[Any, Any]]
      val groupByOp = GroupByOp[Any, Any](groupByFn)
      val fn = mock[FunctionRunner[Any, Any]]
      val chainableOp = mock[ChainableOp[Any, Any]]
      when(chainableOp.fn).thenReturn(fn)

      val chainedOp = groupByOp.chain(chainableOp)
      chainedOp shouldBe a[GroupByOp[_, _]]

      unchainableOps.foreach { op =>
        intercept[OpChainException] {
          groupByOp.chain(op)
        }
      }
    }

    "delegate to groupByFn on getProcessor" in {
      val groupByFn = mock[GroupByFn[Any, Any]]
      val groupByOp = GroupByOp[Any, Any](groupByFn)

      groupByOp.getProcessor
      verify(groupByFn).getProcessor(anyInt, anyString, any[UserConfig])(any[ActorSystem])
    }
  }

  "MergeOp" should {

    val mergeOp = MergeOp("merge")

    "chain ChainableOp" in {
      val fn = mock[FunctionRunner[Any, Any]]
      val chainableOp = mock[ChainableOp[Any, Any]]
      when(chainableOp.fn).thenReturn(fn)

      val chainedOp = mergeOp.chain(chainableOp)
      chainedOp shouldBe a [MergeOp]

      unchainableOps.foreach { op =>
        intercept[OpChainException] {
          mergeOp.chain(op)
        }
      }
    }

    "get Processor" in {
      val processor = mergeOp.getProcessor
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
