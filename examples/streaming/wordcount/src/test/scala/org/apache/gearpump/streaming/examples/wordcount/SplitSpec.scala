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

import akka.actor.ActorSystem
import org.apache.gearpump.Message
import akka.testkit.TestProbe
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.streaming.MockUtil
import org.mockito.Mockito._
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class SplitSpec extends WordSpec with Matchers {

  "Split" should {
    "split the text and deliver to next task" in {

      val taskContext = MockUtil.mockTaskContext

      implicit val system: ActorSystem = ActorSystem("test", TestUtil.DEFAULT_CONFIG)

      val mockTaskActor = TestProbe()

      when(taskContext.self).thenReturn(mockTaskActor.ref)

      val split = new Split
      split.open(taskContext, Instant.now())
      split.read() shouldBe a[Message]
      split.close()
      split.getWatermark
      system.terminate()
      Await.result(system.whenTerminated, Duration.Inf)
    }
  }
}