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
package org.apache.gearpump.cluster

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.TestActorRef
import com.typesafe.config.ConfigValueFactory
import org.apache.gearpump.cluster.AppMasterToMaster.GetAllWorkers
import org.apache.gearpump.cluster.MasterToAppMaster.WorkerList
import org.apache.gearpump.cluster.master.Master
import org.apache.gearpump.cluster.worker.Worker
import org.apache.gearpump.util.Constants

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class MiniCluster {
  private val mockMasterIP = "127.0.0.1"

  implicit val system = ActorSystem("system", TestUtil.MASTER_CONFIG.
    withValue(Constants.NETTY_TCP_HOSTNAME, ConfigValueFactory.fromAnyRef(mockMasterIP)))

  val (mockMaster, worker) = {
    val master = system.actorOf(Props(classOf[Master]), "master")
    val worker = system.actorOf(Props(classOf[Worker], master), "worker")

    // Wait until worker register itself to master
    waitUtilWorkerIsRegistered(master)
    (master, worker)
  }

  def launchActor(props: Props): TestActorRef[Actor] = {
    TestActorRef(props)
  }

  private def waitUtilWorkerIsRegistered(master: ActorRef): Unit = {
    while (!isWorkerRegistered(master)) {}
  }

  private def isWorkerRegistered(master: ActorRef): Boolean = {
    import scala.concurrent.duration._
    implicit val dispatcher = system.dispatcher

    implicit val futureTimeout = Constants.FUTURE_TIMEOUT

    val workerListFuture = (master ? GetAllWorkers).asInstanceOf[Future[WorkerList]]

    // Waits until the worker is registered.
    val workers = Await.result[WorkerList](workerListFuture, 15.seconds)
    workers.workers.size > 0
  }

  def shutDown(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }
}
