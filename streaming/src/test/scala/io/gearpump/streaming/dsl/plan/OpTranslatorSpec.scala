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

package io.gearpump.streaming.dsl.plan

import akka.actor.ActorSystem
import io.gearpump.Message
import io.gearpump.cluster.{TestUtil, UserConfig}
import io.gearpump.streaming.Constants._
import io.gearpump.streaming.MockUtil
import io.gearpump.streaming.dsl.CollectionDataSource
import io.gearpump.streaming.dsl.plan.OpTranslator._
import io.gearpump.streaming.task.StartTime
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest._

class OpTranslatorSpec  extends FlatSpec with Matchers with BeforeAndAfterAll {

  "andThen" should "chain multiple single input function" in {
    val dummy = new DummyInputFunction[String]
    val split = new FlatMapFunction[String, String](line => line.split("\\s"), "split")

    val filter = new FlatMapFunction[String, String](word => if (word.isEmpty) None else Some(word), "filter")

    val map = new FlatMapFunction[String, Int](word => Some(1), "map")

    val sum = new ReduceFunction[Int]({ (left, right) => left + right}, "sum")

    val all = dummy.andThen(split).andThen(filter).andThen(map).andThen(sum)


    assert(all.description == "split.filter.map.sum")

    val data  =
      """
      five  four three  two    one
      five  four three  two
      five  four three
      five  four
      five
      """
    val count = all.process(data).toList.last
    assert(count == 15)
  }

  "Source" should "iterate over input source and apply attached operator" in {

    val taskContext = MockUtil.mockTaskContext

    val conf = UserConfig.empty
    val data = "one two three".split("\\s")

    //source with no transformer
    val source = new SourceTask[String,String](new CollectionDataSource[String](data), None, taskContext, conf)
    source.onStart(StartTime(0))
    source.onNext(Message("next"))
    verify(taskContext, times(1)).output(anyObject())

    //source with transformer
    val anotherTaskContext = MockUtil.mockTaskContext
    val double = new FlatMapFunction[String, String](word => List(word, word), "double")
    val another = new SourceTask(new CollectionDataSource[String](data), Some(double), anotherTaskContext, conf)
    another.onStart(StartTime(0))
    another.onNext(Message("next"))
    verify(anotherTaskContext, times(2)).output(anyObject())
  }

  "GroupByTask" should "group input by groupBy Function and apply attached operator for each group" in {

    val data = "1 2  2  3 3  3"

    var map = Map.empty[String, Int]

    val concat = new ReduceFunction[String]({ (left, right) =>
      left + right
    }, "concat")

    implicit val system = ActorSystem("test",  TestUtil.DEFAULT_CONFIG)
    val config = UserConfig.empty.withValue[SingleInputFunction[String, String]](GEARPUMP_STREAMING_OPERATOR, concat)

    val taskContext = MockUtil.mockTaskContext

    val task = new GroupByTask[String, String, String](input => input, taskContext, config)
    task.onStart(StartTime(0))

    val peopleCaptor = ArgumentCaptor.forClass(classOf[Message])

    data.split("\\s+").foreach { word =>
      task.onNext(Message(word))
    }
    verify(taskContext, times(6)).output(peopleCaptor.capture())

    import scala.collection.JavaConverters._

    val values = peopleCaptor.getAllValues().asScala.map(input => input.msg.asInstanceOf[String])
    assert(values.mkString(",") == "1,2,22,3,33,333")
    system.shutdown
  }

  "MergeTask" should "accept two stream and apply the attached operator" in {

    //source with transformer
    val taskContext = MockUtil.mockTaskContext
    val conf = UserConfig.empty
    val double = new FlatMapFunction[String, String](word => List(word, word), "double")
    val task = new TransformTask[String, String](Some(double), taskContext, conf)
    task.onStart(StartTime(0))

    val data = "1 2  2  3 3  3".split("\\s+")

    data.foreach{input =>
      task.onNext(Message(input))
    }

    verify(taskContext, times(data.length * 2)).output(anyObject())
  }
}