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
package org.apache.gearpump.streaming.examples.complexdag

import akka.actor.ActorSystem
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.{TestUtil, UserConfig}
import org.apache.gearpump.streaming.StreamingTestUtil
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._

class SourceSpec extends WordSpec with Matchers {

  "Source" should {
    "Source should send a msg of List[String](classOf[Source].getCanonicalName)" in {
      val system1 = ActorSystem("Source", TestUtil.DEFAULT_CONFIG)
      val system2 = ActorSystem("Reporter", TestUtil.DEFAULT_CONFIG)
      val (_, echo) = StreamingTestUtil.createEchoForTaskActor(classOf[Source].getName, UserConfig.empty, system1, system2)
      echo.expectMsg(10 seconds, Message(List(classOf[Source].getCanonicalName)))
      system1.shutdown()
      system2.shutdown()
    }
  }
}
