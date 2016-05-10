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

package org.apache.gearpump.streaming.examples.state.processor

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import org.mockito.Mockito._
import org.mockito.{Matchers => MockitoMatchers}
import org.scalatest.{Matchers, WordSpec}

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.task.StartTime

class NumberGeneratorProcessorSpec extends WordSpec with Matchers {
  "NumberGeneratorProcessor" should {
    "send random numbers" in {

      val taskContext = MockUtil.mockTaskContext

      implicit val system = ActorSystem("test")

      val mockTaskActor = TestProbe()

      // Mock self ActorRef
      when(taskContext.self).thenReturn(mockTaskActor.ref)

      val conf = UserConfig.empty
      val genNum = new NumberGeneratorProcessor(taskContext, conf)
      genNum.onStart(StartTime(0))
      mockTaskActor.expectMsgType[Message]

      genNum.onNext(Message("next"))
      verify(taskContext).output(MockitoMatchers.any[Message])
      // mockTaskActor.expectMsgType[Message]

      system.terminate()
      Await.result(system.whenTerminated, Duration.Inf)
    }
  }
}
