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

package io.gearpump.experiments.storm.producer

import akka.testkit.TestProbe
import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.experiments.storm.topology.GearpumpStormComponent.GearpumpSpout
import io.gearpump.streaming.MockUtil
import io.gearpump.streaming.task.StartTime
import org.scalatest.mock.MockitoSugar
import org.scalatest.{WordSpec, Matchers}
import org.mockito.Mockito._

class StormProducerSpec extends WordSpec with Matchers with MockitoSugar {

  "StormProducer" should {
    "start GearpumpSpout onStart" in {
      val startTime = mock[StartTime]
      val gearpumpSpout = mock[GearpumpSpout]
      when(gearpumpSpout.getMessageTimeout).thenReturn(None)
      val taskContext = MockUtil.mockTaskContext
      implicit val actorSystem = taskContext.system
      val taskActor = TestProbe()
      when(taskContext.self).thenReturn(taskActor.ref)
      val userConfig = UserConfig.empty
      val stormProducer = new StormProducer(gearpumpSpout, taskContext, userConfig)

      stormProducer.onStart(startTime)

      verify(gearpumpSpout).start(startTime)
      taskActor.expectMsg(Message("start"))
    }

    "pass message to GearpumpBolt onNext" in {
      val message = mock[Message]
      val gearpumpSpout = mock[GearpumpSpout]
      val timeout = 5
      when(gearpumpSpout.getMessageTimeout).thenReturn(Some(timeout))
      val taskContext = MockUtil.mockTaskContext
      implicit val actorSystem = taskContext.system
      val taskActor = TestProbe()
      when(taskContext.self).thenReturn(taskActor.ref)
      val userConfig = UserConfig.empty
      val stormProducer = new StormProducer(gearpumpSpout, taskContext, userConfig)

      stormProducer.onNext(message)

      verify(gearpumpSpout).next(message)
      taskActor.expectMsg(Message("continue"))

      stormProducer.onNext(StormProducer.TIMEOUT)
      verify(gearpumpSpout).timeout(timeout)
    }
  }
}
