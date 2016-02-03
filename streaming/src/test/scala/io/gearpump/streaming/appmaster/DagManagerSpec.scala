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

package io.gearpump.streaming.appmaster

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestProbe
import io.gearpump.cluster.{TestUtil, UserConfig}
import io.gearpump.partitioner.{HashPartitioner, Partitioner}
import io.gearpump.streaming.appmaster.DagManager.{DAGOperationFailed, DAGOperationSuccess, GetLatestDAG, GetTaskLaunchData, LatestDAG, NewDAGDeployed, ReplaceProcessor, TaskLaunchData, WatchChange}
import io.gearpump.streaming.task.{Subscriber, TaskActor}
import io.gearpump.streaming.{DAG, LifeTime, ProcessorDescription, StreamApplication}
import io.gearpump.util.Graph
import io.gearpump.util.Graph._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class DagManagerSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  val hash = Partitioner[HashPartitioner]
  val task1 = ProcessorDescription(id = 1, taskClass = classOf[TaskActor].getName, parallelism = 1)
  val task2 = ProcessorDescription(id = 2, taskClass = classOf[TaskActor].getName, parallelism = 1)
  val graph = Graph(task1 ~ hash ~> task2)
  val dag = DAG(graph)
  implicit var system: ActorSystem = null
  val appId = 0
  val userConfig = UserConfig.empty.withValue(StreamApplication.DAG, graph)

  "DagManager" should {
    import io.gearpump.streaming.appmaster.ClockServiceSpec.Store
    "maintain the dags properly" in {
      val store = new Store

      val dagManager = system.actorOf(Props(new DagManager(appId, userConfig, store, Some(dag))))
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

    "retrieve last stored dag properly" in {
      val store = new Store
      val newGraph = Graph(task1 ~ hash ~> task2 ~> task2)
      val newDag = DAG(newGraph)
      store.put(StreamApplication.DAG, newDag)
      val dagManager = system.actorOf(Props(new DagManager(appId, userConfig, store, Some(dag))))
      val client = TestProbe()
      client.send(dagManager, GetLatestDAG)
      client.expectMsg(LatestDAG(newDag))
    }
  }

  override def afterAll {
    system.shutdown()
    system.awaitTermination()
  }

  override def beforeAll {
    this.system = ActorSystem("DagManagerSpec", TestUtil.DEFAULT_CONFIG)
  }
}
