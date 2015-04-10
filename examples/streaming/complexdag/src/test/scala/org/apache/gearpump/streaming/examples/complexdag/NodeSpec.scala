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
import org.apache.gearpump.streaming.MockUtil._
import org.mockito.ArgumentMatcher
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}

class NodeSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter {

  val context = MockUtil.mockTaskContext

  val node = new Node(context, UserConfig.empty)

  property("Node should send a List[String](classOf[Node].getCanonicalName, classOf[Node].getCanonicalName"){
    val list = List(classOf[Node].getCanonicalName)
    val expected = List(classOf[Node].getCanonicalName,classOf[Node].getCanonicalName)
    node.onNext(Message(list))
    verify(context).output(argMatch[Message](_.msg == expected))
  }
}
