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

package io.gearpump.experiments.storm.processor

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.experiments.storm.topology.GearpumpStormComponent.{GearpumpBolt, GearpumpSpout}
import io.gearpump.streaming.MockUtil
import io.gearpump.streaming.task.StartTime
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{WordSpec, Matchers}

class StormProcessorSpec extends WordSpec with Matchers with MockitoSugar {

  "StormProcessor" should {
    "start GearpumpSpout onStart" in {
      val startTime = mock[StartTime]
      val gearpumpBolt = mock[GearpumpBolt]
      when(gearpumpBolt.getTickFrequency).thenReturn(None)
      val taskContext = MockUtil.mockTaskContext
      val userConfig = UserConfig.empty
      val stormProcessor = new StormProcessor(gearpumpBolt, taskContext, userConfig)

      stormProcessor.onStart(startTime)

      verify(gearpumpBolt).start(startTime)
    }

    "pass message to GearpumpBolt onNext" in {
      val message = mock[Message]
      val gearpumpBolt = mock[GearpumpBolt]
      val freq = 5L
      when(gearpumpBolt.getTickFrequency).thenReturn(Some(freq))
      val taskContext = MockUtil.mockTaskContext
      val userConfig = UserConfig.empty
      val stormProcessor = new StormProcessor(gearpumpBolt, taskContext, userConfig)

      stormProcessor.onNext(message)

      verify(gearpumpBolt).next(message)

      stormProcessor.onNext(StormProcessor.TICK)

      verify(gearpumpBolt).tick(freq)
    }
  }

}

