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
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.streaming.dsl.op.{OpEdge, Op}
import org.apache.gearpump.streaming.dsl.plan.OpTranslator._
import org.mockito.Mockito.when
import org.scalatest._
import org.scalatest.mock.MockitoSugar
class StreamAppSpec  extends FlatSpec with Matchers with BeforeAndAfterAll  with MockitoSugar {

  implicit var system: ActorSystem = null

  override def beforeAll: Unit = {
    system = ActorSystem("test",  TestUtil.DEFAULT_CONFIG)
  }

  override def afterAll: Unit = {
    system.shutdown()
  }


  it should "be able to generate multiple new streams" in {
    val context: ClientContext = mock[ClientContext]
    when(context.system).thenReturn(system)

    val app = StreamApp("dsl", context)
    app.source(List("A"), 1)
    app.source(List("B"), 1)

    assert(app.graph.vertices.size == 2)
  }

  it should "plan the dsl to Processsor(TaskDescription) DAG" in {
    val context: ClientContext = mock[ClientContext]
    when(context.system).thenReturn(system)

    val app = StreamApp("dsl", context)
    val parallism = 3
    app.source(List("A","B","C"), parallism).flatMap(Array(_)).reduce(_+_)
    val task = app.plan.dag.vertices.iterator.next()
    assert(task.taskClass == classOf[SourceTask[_, _]].getName)
    assert(task.parallelism == parallism)
  }

  it should "produce 3 messages" in {
    val context: ClientContext = mock[ClientContext]
    val app = StreamApp("dsl", context)
    val list = List[String](
      "0",
      "1",
      "2"
    )
    val producer = app.source(list, 1, "producer").flatMap(Array(_)).reduce(_+_)
    val task = app.plan.dag.vertices.iterator.next()
      /*
      val task = app.plan.dag.vertices.iterator.map(desc => {
        LOG.info(s"${desc.taskClass}")
      })
      val sum = producer.flatMap(msg => {
        LOG.info("in flatMap")
        assert(msg.msg.isInstanceOf[String])
        val num = msg.msg.asInstanceOf[String].toInt
        Array(num)
      }).reduce(_+_)
      val task = app.plan.dag.vertices.iterator.map(desc => {
        LOG.info(s"${desc.taskClass}")
      })
     */
  }
}
