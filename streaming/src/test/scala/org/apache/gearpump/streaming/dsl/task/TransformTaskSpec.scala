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
package org.apache.gearpump.streaming.dsl.task

import java.time.Instant

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.dsl.plan.functions.FunctionRunner
import org.apache.gearpump.streaming.dsl.task.TransformTask.Transform
import org.apache.gearpump.streaming.source.Watermark
import org.mockito.Mockito.{verify, when}
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks

class TransformTaskSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("TransformTask should setup functions") {
    forAll(Gen.chooseNum[Long](0L, 1000L).map(Instant.ofEpochMilli)) { (startTime: Instant) =>
      val taskContext = MockUtil.mockTaskContext
      implicit val system = MockUtil.system
      val config = UserConfig.empty
      val operator = mock[FunctionRunner[Any, Any]]
      val transform = new Transform[Any, Any](taskContext, Some(operator))
      val sourceTask = new TransformTask[Any, Any](transform, taskContext, config)

      sourceTask.onStart(startTime)

      verify(operator).setup()
    }
  }

  property("TransformTask should process inputs") {
    forAll(Gen.alphaStr) { (str: String) =>
      val taskContext = MockUtil.mockTaskContext
      implicit val system = MockUtil.system
      val config = UserConfig.empty
      val operator = mock[FunctionRunner[Any, Any]]
      val transform = new Transform[Any, Any](taskContext, Some(operator))
      val task = new TransformTask[Any, Any](transform, taskContext, config)
      val msg = Message(str)
      when(operator.process(str)).thenReturn(Some(str))
      when(operator.finish()).thenReturn(None)

      task.onNext(msg)
      task.onWatermarkProgress(Watermark.MAX)

      verify(taskContext).output(Message(str, Watermark.MAX))
    }
  }

  property("TransformTask should teardown functions") {
    val taskContext = MockUtil.mockTaskContext
    implicit val system = MockUtil.system
    val config = UserConfig.empty
    val operator = mock[FunctionRunner[Any, Any]]
    val transform = new Transform[Any, Any](taskContext, Some(operator))
    val task = new TransformTask[Any, Any](transform, taskContext, config)

    task.onStop()

    verify(operator).teardown()
  }
}
