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
package org.apache.gearpump.streaming.appmaster

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.appmaster.ClockServiceSpec.Store
import org.apache.gearpump.streaming.storage.AppDataStore
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.streaming.{DAG, TaskDescription}
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.Graph._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.{Future, Promise}

class ClockServiceSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll{

  def this() = this(ActorSystem("ClockServiceSpec"))

  val task1 = TaskDescription(classOf[TaskActor].getName, 1)
  val task2 = TaskDescription(classOf[TaskActor].getName, 1)
  val dag = DAG(Graph(task1 ~ new HashPartitioner() ~> task2))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "The ClockService" should {
    "maintain a global view of message timestamp in the application" in {
      val store = new Store()

      val startClock  = 100L
      store.put(ClockService.START_CLOCK, startClock)
      val clockService = system.actorOf(Props(new ClockService(dag, store)))
      clockService ! GetLatestMinClock
      expectMsg(LatestMinClock(startClock))

      //task(0,0): clock(101); task(1,0): clock(100)
      clockService ! UpdateClock(TaskId(0, 0), 101)
      //min is 100
      expectMsg(ClockUpdated(100))

      //task(0,0): clock(101); task(1,0): clock(101)
      clockService ! UpdateClock(TaskId(1, 0), 101)

      //min is 101
      expectMsg(ClockUpdated(101))
    }
  }
}

object ClockServiceSpec {

  class Store extends AppDataStore{

    private var map = Map.empty[String, Any]

    def put(key: String, value: Any): Future[Any] = {
      map = map + (key -> value)
      Promise.successful(value).future
    }

    def get(key: String) : Future[Any] = {
      Promise.successful(map.get(key).getOrElse(null)).future
    }
  }
}
