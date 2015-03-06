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

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.MockUtil
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}

class SinkSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter {

  val context = MockUtil.mockTaskContext

  val sink = new Sink(context, UserConfig.empty)

  property("Sink should send a List[String](classOf[Sink].getCanonicalName, classOf[Sink].getCanonicalName"){
    val list = List(classOf[Sink].getCanonicalName)
    val expected = collection.mutable.MutableList(classOf[Sink].getCanonicalName, classOf[Sink].getCanonicalName)
    sink.onNext(Message(list))

    (0 until sink.list.size).map(i => {
      assert(sink.list(i).equals(expected(i)))
    })
  }
}
