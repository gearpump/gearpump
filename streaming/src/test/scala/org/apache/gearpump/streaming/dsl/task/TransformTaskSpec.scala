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
import org.mockito.{Matchers => MockitoMatchers}
import org.mockito.Mockito.{times, verify, when}
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks

class TransformTaskSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  private val timeGen = Gen.chooseNum[Long](Watermark.MIN.toEpochMilli,
    Watermark.MAX.toEpochMilli - 1).map(Instant.ofEpochMilli)
  private val runnerGen = {
    val runner = mock[FunctionRunner[Any, Any]]
    Gen.oneOf(Some(runner), None)
  }

  property("TransformTask should emit on watermark") {
    val msgGen = for {
      str <- Gen.alphaStr.suchThat(!_.isEmpty)
      t <- timeGen
    } yield Message(s"$str:$t", t)
    val msgsGen = Gen.listOfN(10, msgGen)

    forAll(runnerGen, msgsGen) {
      (runner: Option[FunctionRunner[Any, Any]], msgs: List[Message]) =>
        val taskContext = MockUtil.mockTaskContext
        implicit val system = MockUtil.system
        val config = UserConfig.empty
        val transform = new Transform[Any, Any](taskContext, runner)
        val task = new TransformTask[Any, Any](transform, taskContext, config)

        msgs.foreach(task.onNext)

        runner.foreach(r => when(r.finish()).thenReturn(None))
        task.onWatermarkProgress(Watermark.MIN)
        verify(taskContext, times(0)).output(MockitoMatchers.any[Message])

        msgs.foreach { msg =>
          runner.foreach(r =>
            when(r.process(msg.value)).thenReturn(Some(msg.value)))
        }
        task.onWatermarkProgress(Watermark.MAX)

        msgs.foreach { msg =>
          verify(taskContext).output(MockitoMatchers.eq(msg))
        }
    }
  }

}
