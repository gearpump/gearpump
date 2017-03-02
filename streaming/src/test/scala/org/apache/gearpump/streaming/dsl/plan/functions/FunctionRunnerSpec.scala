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
package org.apache.gearpump.streaming.dsl.plan.functions

import java.time.Instant

import akka.actor.ActorSystem
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.{TestUtil, UserConfig}
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.source.{DataSourceTask, Watermark}
import org.apache.gearpump.streaming.Constants._
import org.apache.gearpump.streaming.dsl.api.functions.ReduceFunction
import org.apache.gearpump.streaming.dsl.scalaapi.CollectionDataSource
import org.apache.gearpump.streaming.dsl.scalaapi.functions.FlatMapFunction
import org.apache.gearpump.streaming.dsl.task.TransformTask.Transform
import org.apache.gearpump.streaming.dsl.task.{CountTriggerTask, TransformTask}
import org.apache.gearpump.streaming.dsl.window.api.CountWindows
import org.apache.gearpump.streaming.dsl.window.impl.GroupAlsoByWindow
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.mock.MockitoSugar

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class FunctionRunnerSpec extends WordSpec with Matchers with MockitoSugar {
  import org.apache.gearpump.streaming.dsl.plan.functions.FunctionRunnerSpec._

  "AndThen" should {

    val first = mock[FunctionRunner[R, S]]
    val second = mock[FunctionRunner[S, T]]
    val andThen = AndThen(first, second)

    "chain first and second functions when processing input value" in {
      val input = mock[R]
      val firstOutput = mock[S]
      val secondOutput = mock[T]
      when(first.process(input)).thenReturn(Some(firstOutput))
      when(second.process(firstOutput)).thenReturn(Some(secondOutput))

      andThen.process(input).toList shouldBe List(secondOutput)
    }

    "return chained description" in {
      when(first.description).thenReturn("first")
      when(second.description).thenReturn("second")
      andThen.description shouldBe "first.second"
    }

    "return either first result or second on finish" in {
      val firstResult = mock[S]
      val processedFirst = mock[T]
      val secondResult = mock[T]

      when(first.finish()).thenReturn(Some(firstResult))
      when(second.process(firstResult)).thenReturn(Some(processedFirst))
      andThen.finish().toList shouldBe List(processedFirst)

      when(first.finish()).thenReturn(None)
      when(second.finish()).thenReturn(Some(secondResult))
      andThen.finish().toList shouldBe List(secondResult)
    }

    "set up both functions on setup" in {
      andThen.setup()

      verify(first).setup()
      verify(second).setup()
    }

    "tear down both functions on teardown" in {
      andThen.teardown()

      verify(first).teardown()
      verify(second).teardown()
    }

    "chain multiple single input function" in {
      val split = new FlatMapper[String, String](FlatMapFunction(_.split("\\s")), "split")

      val filter = new FlatMapper[String, String](
        FlatMapFunction(word => if (word.isEmpty) None else Some(word)), "filter")

      val map = new FlatMapper[String, Int](FlatMapFunction(word => Some(1)), "map")

      val sum = new Reducer[Int](ReduceFunction({(left, right) => left + right}), "sum")

      val all = AndThen(split, AndThen(filter, AndThen(map, sum)))

      assert(all.description == "split.filter.map.sum")

      val data =
        """
      five  four three  two    one
      five  four three  two
      five  four three
      five  four
      five
        """
      // force eager evaluation
      all.process(data).toList
      val result = all.finish().toList
      assert(result.nonEmpty)
      assert(result.last == 15)
    }
  }

  "FlatMapper" should {

    val flatMapFunction = mock[FlatMapFunction[R, S]]
    val flatMapper = new FlatMapper[R, S](flatMapFunction, "flatMap")

    "call flatMap function when processing input value" in {
      val input = mock[R]
      flatMapper.process(input)
      verify(flatMapFunction).apply(input)
    }

    "return passed in description" in {
      flatMapper.description shouldBe "flatMap"
    }

    "return None on finish" in {
      flatMapper.finish() shouldBe List.empty[S]
    }

    "set up FlatMapFunction on setup" in {
      flatMapper.setup()

      verify(flatMapFunction).setup()
    }

    "tear down FlatMapFunction on teardown" in {
      flatMapper.teardown()

      verify(flatMapFunction).teardown()
    }
  }

  "ReduceFunction" should {

    "call reduce function when processing input value" in {
      val reduceFunction = mock[ReduceFunction[T]]
      val reducer = new Reducer[T](reduceFunction, "reduce")
      val input1 = mock[T]
      val input2 = mock[T]
      val output = mock[T]

      when(reduceFunction.apply(input1, input2)).thenReturn(output, output)

      reducer.process(input1) shouldBe List.empty[T]
      reducer.process(input2) shouldBe List.empty[T]
      reducer.finish() shouldBe List(output)

      reducer.teardown()
      reducer.process(input1) shouldBe List.empty[T]
      reducer.teardown()
      reducer.process(input2) shouldBe List.empty[T]
      reducer.finish() shouldBe List(input2)
    }

    "return passed in description" in {
      val reduceFunction = mock[ReduceFunction[T]]
      val reducer = new Reducer[T](reduceFunction, "reduce")
      reducer.description shouldBe "reduce"
    }

    "return None on finish" in {
      val reduceFunction = mock[ReduceFunction[T]]
      val reducer = new Reducer[T](reduceFunction, "reduce")
      reducer.finish() shouldBe List.empty[T]
    }

    "set up reduce function on setup" in {
      val reduceFunction = mock[ReduceFunction[T]]
      val reducer = new Reducer[T](reduceFunction, "reduce")
      reducer.setup()

      verify(reduceFunction).setup()
    }

    "tear down reduce function on teardown" in {
      val reduceFunction = mock[ReduceFunction[T]]
      val reducer = new Reducer[T](reduceFunction, "reduce")
      reducer.teardown()

      verify(reduceFunction).teardown()
    }
  }

  "Emit" should {

    val emitFunction = mock[T => Unit]
    val emit = new Emit[T](emitFunction)

    "emit input value when processing input value" in {
      val input = mock[T]

      emit.process(input) shouldBe List.empty[Unit]

      verify(emitFunction).apply(input)
    }

    "return empty description" in {
      emit.description shouldBe ""
    }

    "return None on finish" in {
      emit.finish() shouldBe List.empty[Unit]
    }

    "do nothing on setup" in {
      emit.setup()

      verifyZeroInteractions(emitFunction)
    }

    "do nothing on teardown" in {
      emit.teardown()

      verifyZeroInteractions(emitFunction)
    }
  }

  "Source" should {
    "iterate over input source and apply attached operator" in {

      val taskContext = MockUtil.mockTaskContext
      implicit val actorSystem = MockUtil.system

      val data = "one two three".split("\\s+")
      val dataSource = new CollectionDataSource[String](data)
      val conf = UserConfig.empty.withValue(GEARPUMP_STREAMING_SOURCE, dataSource)

      // Source with no transformer
      val source = new DataSourceTask[String, String](
        taskContext, conf)
      source.onStart(Instant.EPOCH)
      source.onNext(Message("next"))
      source.onWatermarkProgress(Watermark.MAX)
      data.foreach { s =>
        verify(taskContext, times(1)).output(MockUtil.argMatch[Message](
          message => message.msg == s))
      }

      // Source with transformer
      val anotherTaskContext = MockUtil.mockTaskContext
      val double = new FlatMapper[String, String](FlatMapFunction(
        word => List(word, word)), "double")
      val another = new DataSourceTask(anotherTaskContext,
        conf.withValue(GEARPUMP_STREAMING_OPERATOR, double))
      another.onStart(Instant.EPOCH)
      another.onNext(Message("next"))
      another.onWatermarkProgress(Watermark.MAX)
      data.foreach { s =>
        verify(anotherTaskContext, times(2)).output(MockUtil.argMatch[Message](
          message => message.msg == s))
      }
    }
  }

  "CountTriggerTask" should {
    "group input by groupBy Function and " +
      "apply attached operator for each group" in {

      val data = "1 2  2  3 3  3"

      val concat = new Reducer[String](ReduceFunction({ (left, right) =>
        left + right}), "concat")

      implicit val system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
      val config = UserConfig.empty.withValue[FunctionRunner[String, String]](
        GEARPUMP_STREAMING_OPERATOR, concat)

      val taskContext = MockUtil.mockTaskContext

      val groupBy = GroupAlsoByWindow((input: String) => input,
        CountWindows.apply[String](1).accumulating)
      val task = new CountTriggerTask[String, String](groupBy, taskContext, config)
      task.onStart(Instant.EPOCH)

      val peopleCaptor = ArgumentCaptor.forClass(classOf[Message])

      data.split("\\s+").foreach { word =>
        task.onNext(Message(word))
      }
      verify(taskContext, times(6)).output(peopleCaptor.capture())

      import scala.collection.JavaConverters._

      val values = peopleCaptor.getAllValues.asScala.map(input => input.msg.asInstanceOf[String])
      assert(values.mkString(",") == "1,2,22,3,33,333")
      system.terminate()
      Await.result(system.whenTerminated, Duration.Inf)
    }
  }

  "TransformTask" should {
    "accept two stream and apply the attached operator" in {

      // Source with transformer
      val taskContext = MockUtil.mockTaskContext
      val conf = UserConfig.empty
      val double = new FlatMapper[String, String](FlatMapFunction(
        word => List(word, word)), "double")
      val transform = new Transform[String, String](taskContext, Some(double))
      val task = new TransformTask[String, String](transform, taskContext, conf)
      task.onStart(Instant.EPOCH)

      val data = "1 2  2  3 3  3".split("\\s+")

      data.foreach { input =>
        task.onNext(Message(input))
      }

      task.onWatermarkProgress(Watermark.MAX)

      verify(taskContext, times(data.length * 2)).output(anyObject())
    }
  }
}

object FunctionRunnerSpec {
  type R = AnyRef
  type S = AnyRef
  type T = AnyRef
}
