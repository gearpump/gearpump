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
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}

class SinkSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter {
  val system1 = ActorSystem("SinkSpec", TestUtil.DEFAULT_CONFIG)
  val system2 = ActorSystem("Reporter", TestUtil.DEFAULT_CONFIG)
  val (sink, echo) = StreamingTestUtil.createEchoForTaskActor(classOf[Sink].getName, UserConfig.empty, system1, system2)

  property("Sink should send a List[String](classOf[Sink].getCanonicalName, classOf[Sink].getCanonicalName"){
    val list = List(classOf[Sink].getCanonicalName)
    val expected = collection.mutable.MutableList(classOf[Sink].getCanonicalName, classOf[Sink].getCanonicalName)
    sink.tell(Message(list), sink)
    val sinkActor = sink.underlying.actor.asInstanceOf[Sink]
    (0 until sinkActor.list.size).map(i => {
      assert(sinkActor.list(i).equals(expected(i)))
    })
  }

  after {
    system1.shutdown()
    system2.shutdown()
  }
}
