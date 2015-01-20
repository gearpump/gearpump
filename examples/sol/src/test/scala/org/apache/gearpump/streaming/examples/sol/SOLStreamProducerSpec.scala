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
package org.apache.gearpump.streaming.examples.sol

import akka.actor.ActorSystem
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.{UserConfig, TestUtil}
import org.apache.gearpump.streaming.StreamingTestUtil
import org.scalatest.{WordSpec, Matchers}

class SOLStreamProducerSpec extends WordSpec with Matchers {

  "SOLStreamProducer" should {
    "producer message continuously" in {
      val system1 = ActorSystem("SOLStreamProducer", TestUtil.DEFAULT_CONFIG)
      val system2 = ActorSystem("Reporter", TestUtil.DEFAULT_CONFIG)
      val conf = UserConfig.empty.withInt(SOLStreamProducer.BYTES_PER_MESSAGE, 100)
      val (_, echo) = StreamingTestUtil.createEchoForTaskActor(classOf[SOLStreamProducer].getName, conf, system1, system2, usePinedDispatcherForTaskActor = true)
      echo.expectMsgAllClassOf(classOf[Message])
      system1.shutdown()
      system2.shutdown()
    }
  }
}
