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

package io.gearpump.streaming.dsl.plan.functions

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.Constants._
import io.gearpump.streaming.MockUtil
import io.gearpump.streaming.dsl.api.functions.{FoldFunction, ReduceFunction}
import io.gearpump.streaming.dsl.scalaapi.CollectionDataSource
import io.gearpump.streaming.dsl.scalaapi.functions.FlatMapFunction
import io.gearpump.streaming.dsl.task.TransformTask
import io.gearpump.streaming.dsl.window.api.GlobalWindows
import io.gearpump.streaming.dsl.window.impl.{StreamingOperator, WindowOperator}
import io.gearpump.streaming.source.{DataSourceTask, Watermark}
import java.time.Instant
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.mockito.MockitoSugar

class FunctionRunnerSpec extends WordSpec with Matchers with MockitoSugar {
  import io.gearpump.streaming.dsl.plan.functions.FunctionRunnerSpec._

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
      val split = new FlatMapper[String, String](FlatMapFunction(
        (_.split("\\s")): String => TraversableOnce[String]), "split")

      val filter = new FlatMapper[String, String](
        FlatMapFunction((word =>
          if (word.isEmpty) None else Some(word)): String => TraversableOnce[String]), "filter")

      val map = new FlatMapper[String, Int](FlatMapFunction(
        (_ => Some(1)): String => TraversableOnce[Int]), "map")

      val sum = new FoldRunner[Int, Option[Int]](
        ReduceFunction({(left, right) => left + right}), "sum")

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

      all.setup()
      // force eager evaluation
      all.process(data).toList
      val result = all.finish().toList.map(_.get)
      assert(result.nonEmpty)
      assert(result.last == 15)
      all.teardown()
    }
  }

  "FlatMapper" should {

    val flatMapFunction = mock[FlatMapFunction[R, S]]
    val flatMapper = new FlatMapper[R, S](flatMapFunction, "flatMap")

    "call flatMap function when processing input value" in {
      val input = mock[R]
      flatMapper.process(input)
      verify(flatMapFunction).flatMap(input)
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

  "FoldRunner" should {

    "call fold function when processing input value" in {
      val foldFunction = mock[FoldFunction[T, List[T]]]
      val foldRunner = new FoldRunner[T, List[T]](foldFunction, "fold")
      val input1 = mock[T]
      val input2 = mock[T]

      when(foldFunction.init).thenReturn(Nil)
      when(foldFunction.fold(Nil, input1)).thenReturn(List(input1))
      when(foldFunction.fold(Nil, input2)).thenReturn(List(input2))
      when(foldFunction.fold(List(input1), input2)).thenReturn(List(input1, input2))

      foldRunner.setup()
      foldRunner.process(input1) shouldBe List.empty[T]
      foldRunner.process(input2) shouldBe List.empty[T]
      foldRunner.finish() shouldBe List(List(input1, input2))
      foldRunner.teardown()

      foldRunner.setup()
      foldRunner.process(input1) shouldBe List.empty[T]
      foldRunner.teardown()
      foldRunner.setup()
      foldRunner.process(input2) shouldBe List.empty[T]
      foldRunner.finish() shouldBe List(List(input2))
    }

    "return passed in description" in {
      val foldFunction = mock[FoldFunction[S, T]]
      val foldRunner = new FoldRunner[S, T](foldFunction, "fold")
      foldRunner.description shouldBe "fold"
    }

    "return None on finish" in {
      val foldFunction = mock[FoldFunction[S, T]]
      val foldRunner = new FoldRunner[S, T](foldFunction, "fold")
      foldRunner.finish() shouldBe List.empty[T]
    }

    "set up fold function on setup" in {
      val foldFunction = mock[FoldFunction[S, T]]
      val foldRunner = new FoldRunner[S, T](foldFunction, "fold")
      foldRunner.setup()

      verify(foldFunction).setup()
    }

    "tear down fold function on teardown" in {
      val foldFunction = mock[FoldFunction[S, T]]
      val foldRunner = new FoldRunner[S, T](foldFunction, "fold")
      foldRunner.teardown()

      verify(foldFunction).teardown()
    }
  }

  "Source" should {
    "iterate over input source and apply attached operator" in {

      val taskContext = MockUtil.mockTaskContext
      implicit val actorSystem = MockUtil.system

      val data = "one two three".split("\\s+")
      val dataSource = new CollectionDataSource[String](data)
      val runner1 = new WindowOperator[String, String](
        GlobalWindows(), new DummyRunner[String])
      val conf = UserConfig.empty
        .withValue(GEARPUMP_STREAMING_SOURCE, dataSource)
        .withValue[StreamingOperator[String, String]](GEARPUMP_STREAMING_OPERATOR, runner1)

      // Source with no transformer
      val source = new DataSourceTask[String, String](
        taskContext, conf)
      source.onStart(Instant.EPOCH)
      source.onNext(Message("next"))
      source.onWatermarkProgress(Watermark.MAX)
      data.foreach { s =>
        verify(taskContext, times(1)).output(MockUtil.argMatch[Message](
          message => message.value == s))
      }

      // Source with transformer
      val anotherTaskContext = MockUtil.mockTaskContext
      val double = new FlatMapper[String, String](FlatMapFunction(
        (word => List(word, word)): String => TraversableOnce[String]), "double")
      val runner2 = new WindowOperator[String, String](
        GlobalWindows(), double)
      val another = new DataSourceTask(anotherTaskContext,
        conf.withValue(GEARPUMP_STREAMING_OPERATOR, runner2))
      another.onStart(Instant.EPOCH)
      another.onNext(Message("next"))
      another.onWatermarkProgress(Watermark.MAX)
      data.foreach { s =>
        verify(anotherTaskContext, times(2)).output(MockUtil.argMatch[Message](
          message => message.value == s))
      }
    }
  }


  "MergeTask" should {
    "accept two stream and apply the attached operator" in {

      // Source with transformer
      val taskContext = MockUtil.mockTaskContext
      val conf = UserConfig.empty
      val double = new FlatMapper[String, String](FlatMapFunction(
        (word => List(word, word)): String => List[String]), "double")
      val transform = new WindowOperator[String, String](GlobalWindows(), double)
      val task = new TransformTask[String, String](transform, taskContext, conf)
      task.onStart(Instant.EPOCH)

      val data = "1 2  2  3 3  3".split("\\s+")

      data.foreach { input =>
        task.onNext(Message(input))
      }

      task.onWatermarkProgress(Watermark.MAX)

      verify(taskContext, times(data.length * 2)).output(any[Message])
    }
  }
}

object FunctionRunnerSpec {
  type R = AnyRef
  type S = AnyRef
  type T = AnyRef
}
