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

package org.apache.gearpump.streaming.dsl

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.streaming.dsl.plan.OpTranslator._
import org.scalatest._

class StreamAppSpec  extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit var system: ActorSystem = null
  implicit var context: ClientContext = null

  override def beforeAll: Unit = {
    system = ActorSystem("test")
    context = ClientContext()
  }

  override def afterAll: Unit = {
    system.shutdown()
  }


  it should "be able to generate multiple new streams" in {
    val app = new StreamApp("dsl", context)
    app.fromCollection(List("A"), 1)
    app.fromCollection(List("B"), 1)

    assert(app.graph.vertices.size == 2)
  }

  it should "plan the dsl to Processsor(TaskDescription) DAG" in {
    val app = new StreamApp("dsl", context)
    val parallism = 3
    app.fromCollection(List("A"), parallism)
    val task = app.plan.dag.vertices.iterator.next()
    assert(task.taskClass == classOf[SourceTask[_, _]].getName)
    assert(task.parallelism == parallism)
  }
}