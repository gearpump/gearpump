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

package org.apache.gearpump.akkastream.example

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.scaladsl.{Sink, Source}
import org.apache.gearpump.akkastream.GearpumpMaterializer
import org.apache.gearpump.akkastream.graph.GraphPartitioner
import org.apache.gearpump.cluster.main.ArgumentsParser
import org.apache.gearpump.util.AkkaApp

import scala.concurrent.Await
import scala.concurrent.duration._


/**
 * Source and Sink are materialized locally.
 * Remaining GraphStages are materialized remotely:
 *  statefulMap, filter, fold, flatMap
 */
object Test extends AkkaApp with ArgumentsParser {
  // scalastyle:off println
  override def main(akkaConf: Config, args: Array[String]): Unit = {
    implicit val system = ActorSystem("Test", akkaConf)
    implicit val materializer = GearpumpMaterializer(GraphPartitioner.AllRemoteStrategy)

    val echo = system.actorOf(Props(new Echo()))
    val sink = Sink.actorRef(echo, "COMPLETE")

    Source(
      List("red hat", "yellow sweater", "blue jack", "red apple", "green plant", "blue sky")
    ).filter(_.startsWith("red")).fold("Items:") {(a, b) =>
      a + "|" + b
    }.map("I want to order item: " + _).runWith(sink)

    Await.result(system.whenTerminated, 60.minutes)
  }

  class Echo extends Actor {
    def receive: Receive = {
      case any: AnyRef =>
        println("Confirm received: " + any)
    }
  }
  // scalastyle:on println
}
