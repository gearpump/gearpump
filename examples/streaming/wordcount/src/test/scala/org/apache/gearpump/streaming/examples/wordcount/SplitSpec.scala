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
package org.apache.gearpump.streaming.examples.wordcount

import java.time.Instant

import org.apache.gearpump.streaming.source.Watermark

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{Matchers, WordSpec}

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.{TestUtil, UserConfig}
import org.apache.gearpump.streaming.MockUtil

class SplitSpec extends WordSpec with Matchers {

  "Split" should {
    "split the text and deliver to next task" in {

      val taskContext = MockUtil.mockTaskContext

      implicit val system: ActorSystem = ActorSystem("test", TestUtil.DEFAULT_CONFIG)

      val mockTaskActor = TestProbe()

      // Mock self ActorRef
      when(taskContext.self).thenReturn(mockTaskActor.ref)

      val conf = UserConfig.empty
      val split = new Split(taskContext, conf)
      split.onStart(Instant.EPOCH)
      mockTaskActor.expectMsgType[Watermark]

      val expectedWordCount = Split.TEXT_TO_SPLIT.split( """[\s\n]+""").count(_.nonEmpty)

      split.onNext(Message("next"))
      verify(taskContext, times(expectedWordCount)).output(anyObject())

      system.terminate()
      Await.result(system.whenTerminated, Duration.Inf)
    }
  }
}