/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.dsl.scalaapi

import akka.actor._
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.{TestUtil, UserConfig}
import org.apache.gearpump.streaming.dsl.scalaapi.StreamSpec.Join
import org.apache.gearpump.streaming.dsl.task.{GroupByTask, TransformTask}
import org.apache.gearpump.streaming.partitioner.{CoLocationPartitioner, GroupByPartitioner, HashPartitioner, PartitionerDescription}
import org.apache.gearpump.streaming.source.DataSourceTask
import org.apache.gearpump.streaming.task.{Task, TaskContext}
import org.apache.gearpump.streaming.{ProcessorDescription, StreamApplication}
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.Graph._
import org.mockito.Mockito.when
import org.scalatest._
import org.scalatest.mock.MockitoSugar

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Either, Left, Right}

class StreamSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  implicit var system: ActorSystem = _

  override def beforeAll(): Unit = {
    system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
  }

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }

  it should "translate the DSL to a DAG" in {
    val context: ClientContext = mock[ClientContext]
    when(context.system).thenReturn(system)

    val dsl = StreamApp("dsl", context)

    val data =
      """
        five  four three  two    one
        five  four three  two
        five  four three
        five  four
        five
      """
    val stream = dsl.source(data.lines.toList, 1, "").
      flatMap(line => line.split("[\\s]+")).filter(_.nonEmpty).
      map(word => (word, 1)).
      groupBy(_._1, parallelism = 2).
      reduce((left, right) => (left._1, left._2 + right._2)).
      map[Either[(String, Int), String]]({t: (String, Int) => Left(t)})

    val query = dsl.source(List("two"), 1, "").map[Either[(String, Int), String]](
      {s: String => Right(s)})
    stream.merge(query).process[(String, Int)](classOf[Join], 1)

    val app: StreamApplication = dsl.plan()
    val dag = app.userConfig
      .getValue[Graph[ProcessorDescription, PartitionerDescription]](StreamApplication.DAG).get

    val dagTopology = dag.mapVertex(_.taskClass).mapEdge { (node1, edge, node2) =>
      edge.partitionerFactory.partitioner.getClass.getName
    }
    val expectedDagTopology = getExpectedDagTopology

    dagTopology.getVertices.toSet should
      contain theSameElementsAs expectedDagTopology.getVertices.toSet
    dagTopology.getEdges.toSet should
      contain theSameElementsAs expectedDagTopology.getEdges.toSet
  }

  private def getExpectedDagTopology: Graph[String, String] = {
    val source = classOf[DataSourceTask[_, _]].getName
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

  class Join(taskContext: TaskContext, userConf: UserConfig) extends Task(taskContext, userConf) {

    var query: String = _

    override def onNext(msg: Message): Unit = {
      msg.value match {
        case Left(wordCount: (String @unchecked, Int @unchecked)) =>
          if (query != null && wordCount._1 == query) {
            taskContext.output(Message(wordCount))
          }

        case Right(query: String) =>
          this.query = query
      }
    }
  }
}
