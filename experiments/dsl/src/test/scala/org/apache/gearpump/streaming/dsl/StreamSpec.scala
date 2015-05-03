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

import akka.actor._
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.{TestUtil, UserConfig}
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.partitioner.{CoLocationPartitioner, HashPartitioner}
import org.apache.gearpump.streaming.dsl.StreamSpec.Join
import org.apache.gearpump.streaming.dsl.op.{Op, OpEdge}
import org.apache.gearpump.streaming.dsl.partitioner.GroupByPartitioner
import org.apache.gearpump.streaming.dsl.plan.OpTranslator._
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.Graph._
import org.mockito.Mockito.when
import org.scalatest._
import org.scalatest.mock.MockitoSugar

import scala.util.{Either, Left, Right}

class StreamSpec  extends FlatSpec with Matchers with BeforeAndAfterAll  with MockitoSugar {

  implicit var system: ActorSystem = null

  override def beforeAll: Unit = {
    system = ActorSystem("test",  TestUtil.DEFAULT_CONFIG)
  }

  override def afterAll(): Unit = {
    system.shutdown()
  }

  it should "translate the DSL to a DAG" in {
    val context: ClientContext = mock[ClientContext]
    when(context.system).thenReturn(system)

    val app = StreamApp("dsl", context)

    val data  =
      """
        five  four three  two    one
        five  four three  two
        five  four three
        five  four
        five
      """
    val stream = app.source(data.lines.toList, 1).
      flatMap(line => line.split("[\\s]+")).filter(_.nonEmpty).
      map(word => (word, 1)).
      groupBy(_._1, parallelism = 2).
      reduce((left, right) => (left._1, left._2 + right._2)).
      map[Either[(String, Int), String]](Left(_))

    val query = app.source(List("two"), 1).map[Either[(String, Int), String]](Right(_))
    stream.merge(query).process[(String, Int)](classOf[Join], 1)

    val appDescription = app.plan

    val dagTopology = appDescription.dag.mapVertex(_.taskClass).mapEdge((node1, edge, node2) => edge.getClass.getName)
    val expectedDagTopology = getExpectedDagTopology

    assert(dagTopology.vertices.toSet.equals(expectedDagTopology.vertices.toSet))
    assert(dagTopology.edges.toSet.equals(expectedDagTopology.edges.toSet))
  }

  private def getExpectedDagTopology: Graph[String, String] = {
    val source = classOf[SourceTask[_, _]].getName
    val group = classOf[GroupByTask[_, _, _]].getName
    val merge = classOf[TransformTask[_, _]].getName
    val join = classOf[Join].getName

    val hash = classOf[HashPartitioner].getName
    val groupBy = classOf[GroupByPartitioner[_, _]].getName
    val colocation = classOf[CoLocationPartitioner].getName

    val expectedDagTopology = Graph(
      source ~ groupBy ~> group ~ colocation ~> merge ~ hash ~> join,
      source ~ hash ~> merge
    )
    expectedDagTopology
  }
}

object StreamSpec {

  class Join(taskContext : TaskContext, userConf : UserConfig) extends Task(taskContext, userConf) {

    var query: String = null
    override def onStart(startTime: StartTime): Unit = {}

    override def onNext(msg: Message): Unit = {
      msg.msg match {
        case Left(wordCount: (String, Int)) =>
          if (query != null && wordCount._1 == query) {
            taskContext.output(new Message(wordCount))
          }

        case Right(query: String) =>
          this.query = query
      }
    }
  }
}
