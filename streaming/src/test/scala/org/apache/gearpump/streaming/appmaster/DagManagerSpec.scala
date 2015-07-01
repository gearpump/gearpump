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

package org.apache.gearpump.streaming.appmaster

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.partitioner.{PartitionerDescription, HashPartitioner, Partitioner}
import org.apache.gearpump.streaming.appmaster.DagManager.{NewDAGDeployed, DAGOperationFailed, DAGOperationSuccess, ReplaceProcessor, WatchChange, LatestDAG, TaskLaunchData, GetTaskLaunchData, GetLatestDAG}
import org.apache.gearpump.streaming.{LifeTime, StreamApplication, DAG, ProcessorDescription}
import org.apache.gearpump.streaming.task.{Subscriber, TaskActor}
import org.apache.gearpump.util.Graph
import org.scalatest.{FlatSpec, BeforeAndAfterAll, Matchers, WordSpec}
import akka.actor.Props
import org.apache.gearpump.cluster.UserConfig
import akka.testkit.TestProbe
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.Graph._


class DagManagerSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  val hash = Partitioner[HashPartitioner]
  val task1 = ProcessorDescription(id = 1, classOf[TaskActor].getName, 1)
  val task2 = ProcessorDescription(id = 2, classOf[TaskActor].getName, 1)
  val graph = Graph(task1 ~ hash ~> task2)
  val dag = DAG(graph)
  implicit var system: ActorSystem = null
  val appId = 0
  val userConfig = UserConfig.empty.withValue(StreamApplication.DAG, graph)

  it should "" in {

    val dagManager = system.actorOf(Props(new DagManager(appId,
      userConfig, Some(dag))))
    val client = TestProbe()
    client.send(dagManager, GetLatestDAG)
    client.expectMsg(LatestDAG(dag))

    client.send(dagManager, GetTaskLaunchData(dag.version, task1.id, null))
    val task1LaunchData = TaskLaunchData(task1, Subscriber.of(task1.id, dag))
    client.expectMsg(task1LaunchData)

    val task2LaunchData = TaskLaunchData(task2, Subscriber.of(task2.id, dag))
    client.send(dagManager, GetTaskLaunchData(dag.version, task2.id, null))
    client.expectMsg(task2LaunchData)

    val watcher = TestProbe()
    client.send(dagManager, WatchChange(watcher.ref))
    val task3 = task2.copy(id = 3, life = LifeTime(100, Long.MaxValue))

    client.send(dagManager, ReplaceProcessor(task2.id, task3))
    client.expectMsg(DAGOperationSuccess)


    client.send(dagManager, GetLatestDAG)
    val newDag = client.expectMsgPF() {
      case LatestDAG(dag) => dag
    }
    assert(newDag.processors.contains(task3.id))
    watcher.expectMsgType[LatestDAG]

    val task4 = task3.copy(id = 4)
    client.send(dagManager, ReplaceProcessor(task3.id, task4))
    client.expectMsgType[DAGOperationFailed]

    client.send(dagManager, NewDAGDeployed(newDag.version))
    client.send(dagManager, ReplaceProcessor(task3.id, task4))
    client.expectMsg(DAGOperationSuccess)
  }

  override def afterAll {
    system.shutdown()
  }

  override def beforeAll {
    this.system = ActorSystem("DagManagerSpec", TestUtil.DEFAULT_CONFIG)
  }
}
