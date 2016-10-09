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
import org.apache.gearpump.streaming.dsl.CollectionDataSource
import org.apache.gearpump.streaming.source.DataSourceTask
import org.apache.gearpump.streaming.Constants._
import org.apache.gearpump.streaming.dsl.task.{CountTriggerTask, TransformTask}
import org.apache.gearpump.streaming.dsl.window.api.CountWindow
import org.apache.gearpump.streaming.dsl.window.impl.GroupAlsoByWindow
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.mock.MockitoSugar

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class SingleInputFunctionSpec extends WordSpec with Matchers with MockitoSugar {
  import org.apache.gearpump.streaming.dsl.plan.functions.SingleInputFunctionSpec._

  "AndThen" should {

    val first = mock[SingleInputFunction[R, S]]
    val second = mock[SingleInputFunction[S, T]]
    val andThen = new AndThen(first, second)

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

    "clear both states on clearState" in {
      andThen.clearState()

      verify(first).clearState()
      verify(second).clearState()
    }

    "return AndThen on andThen" in {
      val third = mock[SingleInputFunction[T, Any]]
      andThen.andThen[Any](third) shouldBe an [AndThen[_, _, _]]
    }
  }

  "FlatMapFunction" should {

    val flatMap = mock[R => TraversableOnce[S]]
    val flatMapFunction = new FlatMapFunction[R, S](flatMap, "flatMap")

    "call flatMap function when processing input value" in {
      val input = mock[R]
      flatMapFunction.process(input)
      verify(flatMap).apply(input)
    }

    "return passed in description" in {
      flatMapFunction.description shouldBe "flatMap"
    }

    "return None on finish" in {
      flatMapFunction.finish() shouldBe List.empty[S]
    }

    "do nothing on clearState" in {
      flatMapFunction.clearState()
      verifyZeroInteractions(flatMap)
    }

    "return AndThen on andThen" in {
      val other = mock[SingleInputFunction[S, T]]
      flatMapFunction.andThen[T](other) shouldBe an [AndThen[_, _, _]]
    }
  }

  "ReduceFunction" should {


    "call reduce function when processing input value" in {
      val reduce = mock[(T, T) => T]
      val reduceFunction = new ReduceFunction[T](reduce, "reduce")
      val input1 = mock[T]
      val input2 = mock[T]
      val output = mock[T]

      when(reduce.apply(input1, input2)).thenReturn(output, output)

      reduceFunction.process(input1) shouldBe List.empty[T]
      reduceFunction.process(input2) shouldBe List.empty[T]
      reduceFunction.finish() shouldBe List(output)

      reduceFunction.clearState()
      reduceFunction.process(input1) shouldBe List.empty[T]
      reduceFunction.clearState()
      reduceFunction.process(input2) shouldBe List.empty[T]
      reduceFunction.finish() shouldBe List(input2)
    }

    "return passed in description" in {
      val reduce = mock[(T, T) => T]
      val reduceFunction = new ReduceFunction[T](reduce, "reduce")
      reduceFunction.description shouldBe "reduce"
    }

    "return None on finish" in {
      val reduce = mock[(T, T) => T]
      val reduceFunction = new ReduceFunction[T](reduce, "reduce")
      reduceFunction.finish() shouldBe List.empty[T]
    }

    "do nothing on clearState" in {
      val reduce = mock[(T, T) => T]
      val reduceFunction = new ReduceFunction[T](reduce, "reduce")
      reduceFunction.clearState()
      verifyZeroInteractions(reduce)
    }

    "return AndThen on andThen" in {
      val reduce = mock[(T, T) => T]
      val reduceFunction = new ReduceFunction[T](reduce, "reduce")
      val other = mock[SingleInputFunction[T, Any]]
      reduceFunction.andThen[Any](other) shouldBe an[AndThen[_, _, _]]
    }
  }

  "EmitFunction" should {

    val emit = mock[T => Unit]
    val emitFunction = new EmitFunction[T](emit)

    "emit input value when processing input value" in {
      val input = mock[T]

      emitFunction.process(input) shouldBe List.empty[Unit]

      verify(emit).apply(input)
    }

    "return empty description" in {
      emitFunction.description shouldBe ""
    }

    "return None on finish" in {
      emitFunction.finish() shouldBe List.empty[Unit]
    }

    "do nothing on clearState" in {
      emitFunction.clearState()
      verifyZeroInteractions(emit)
    }

    "throw exception on andThen" in {
      val other = mock[SingleInputFunction[Unit, Any]]
      intercept[UnsupportedOperationException] {
        emitFunction.andThen(other)
      }
    }
  }

  "andThen" should {
    "chain multiple single input function" in {
      val split = new FlatMapFunction[String, String](line => line.split("\\s"), "split")

      val filter = new FlatMapFunction[String, String](word =>
        if (word.isEmpty) None else Some(word), "filter")

      val map = new FlatMapFunction[String, Int](word => Some(1), "map")

      val sum = new ReduceFunction[Int]({ (left, right) => left + right }, "sum")

      val all = split.andThen(filter).andThen(map).andThen(sum)

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

  "Source" should {
    "iterate over input source and apply attached operator" in {

      val taskContext = MockUtil.mockTaskContext
      implicit val actorSystem = MockUtil.system

      val data = "one two three".split("\\s")
      val dataSource = new CollectionDataSource[String](data)
      val conf = UserConfig.empty.withValue(GEARPUMP_STREAMING_SOURCE, dataSource)

      // Source with no transformer
      val source = new DataSourceTask[String, String](
        taskContext, conf)
      source.onStart(Instant.EPOCH)
      source.onNext(Message("next"))
      data.foreach { s =>
        verify(taskContext, times(1)).output(MockUtil.argMatch[Message](
          message => message.msg == s))
      }

      // Source with transformer
      val anotherTaskContext = MockUtil.mockTaskContext
      val double = new FlatMapFunction[String, String](word => List(word, word), "double")
      val another = new DataSourceTask(anotherTaskContext,
        conf.withValue(GEARPUMP_STREAMING_OPERATOR, double))
      another.onStart(Instant.EPOCH)
      another.onNext(Message("next"))
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

      val concat = new ReduceFunction[String]({ (left, right) =>
        left + right
      }, "concat")

      implicit val system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
      val config = UserConfig.empty.withValue[SingleInputFunction[String, String]](
        GEARPUMP_STREAMING_OPERATOR, concat)

      val taskContext = MockUtil.mockTaskContext

      val groupBy = GroupAlsoByWindow((input: String) => input, CountWindow.apply(1).accumulating)
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

  "MergeTask" should {
    "accept two stream and apply the attached operator" in {

      // Source with transformer
      val taskContext = MockUtil.mockTaskContext
      val conf = UserConfig.empty
      val double = new FlatMapFunction[String, String](word => List(word, word), "double")
      val task = new TransformTask[String, String](Some(double), taskContext, conf)
      task.onStart(Instant.EPOCH)

      val data = "1 2  2  3 3  3".split("\\s+")

      data.foreach { input =>
        task.onNext(Message(input))
      }

      verify(taskContext, times(data.length * 2)).output(anyObject())
    }
  }
}

object SingleInputFunctionSpec {
  type R = AnyRef
  type S = AnyRef
  type T = AnyRef
}
