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
package gearpump.streaming.examples.complexdag

import gearpump.Message
import gearpump.streaming.MockUtil
import gearpump.streaming.MockUtil._
import gearpump.cluster.UserConfig
import org.mockito.Mockito._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}

class NodeSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter {

  val context = MockUtil.mockTaskContext

  val node = new Node(context, UserConfig.empty)

  property("Node should send a Vector[String](classOf[Node].getCanonicalName, classOf[Node].getCanonicalName"){
    val list = Vector(classOf[Node].getCanonicalName)
    val expected = Vector(classOf[Node].getCanonicalName,classOf[Node].getCanonicalName)
    node.onNext(Message(list))
    verify(context).output(argMatch[Message](_.msg == expected))
  }
}
