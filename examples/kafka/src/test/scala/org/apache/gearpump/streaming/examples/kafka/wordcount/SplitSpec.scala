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
package org.apache.gearpump.streaming.examples.kafka.wordcount

import akka.actor.ActorSystem
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.{UserConfig, TestUtil}
import org.apache.gearpump.streaming.StreamingTestUtil
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfter, PropSpec, Matchers}

import scala.concurrent.duration._

class SplitSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter{

  val system1 = ActorSystem("Split", TestUtil.DEFAULT_CONFIG)
  val system2 = ActorSystem("Reporter", TestUtil.DEFAULT_CONFIG)
  val (split, echo) = StreamingTestUtil.createEchoForTaskActor(classOf[Split].getName, UserConfig.empty, system1, system2)

  property("Split should split the text and deliver to next task"){
    for(txt <- List("this is a test message")) {
      split.tell(Message(txt), split)
      txt.split("\\s+").foreach { msg =>
        echo.expectMsg(10 seconds, Message(msg))
      }
    }
  }

  after {
    system1.shutdown()
    system2.shutdown()
  }
}
