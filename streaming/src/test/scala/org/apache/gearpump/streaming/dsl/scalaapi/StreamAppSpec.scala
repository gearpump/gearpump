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

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.streaming.dsl.scalaapi
import org.apache.gearpump.streaming.partitioner.PartitionerDescription
import org.apache.gearpump.streaming.source.DataSourceTask
import org.apache.gearpump.streaming.{ProcessorDescription, StreamApplication}
import org.apache.gearpump.util.Graph
import org.mockito.Mockito.when
import org.scalatest._
import org.scalatest.mock.MockitoSugar

import scala.concurrent.Await
import scala.concurrent.duration.Duration
class StreamAppSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  implicit var system: ActorSystem = _

  override def beforeAll(): Unit = {
    system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
  }

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }

  it should "be able to generate multiple new streams" in {
    val context: ClientContext = mock[ClientContext]
    when(context.system).thenReturn(system)

    val dsl = StreamApp("dsl", context)
    dsl.source(List("A"), 2, "A") shouldBe a [scalaapi.Stream[_]]
    dsl.source(List("B"), 3, "B") shouldBe a [scalaapi.Stream[_]]

    val application = dsl.plan()
    application shouldBe a [StreamApplication]
    application.name shouldBe "dsl"
    val dag = application.userConfig
      .getValue[Graph[ProcessorDescription, PartitionerDescription]](StreamApplication.DAG).get
    dag.getVertices.size shouldBe 2
    dag.getVertices.foreach { processor =>
      processor.taskClass shouldBe classOf[DataSourceTask[_, _]].getName
      if (processor.description == "A.globalWindows") {
        processor.parallelism shouldBe 2
      } else if (processor.description == "B.globalWindows") {
        processor.parallelism shouldBe 3
      } else {
        fail(s"undefined source ${processor.description}")
      }
    }
  }
}
