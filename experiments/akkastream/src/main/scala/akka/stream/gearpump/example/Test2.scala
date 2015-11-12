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

package akka.stream.gearpump.example

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.gearpump.GearpumpMaterializer
import akka.stream.gearpump.scaladsl.{GearSink, GearSource}
import akka.stream.scaladsl.{Flow, FlowGraph, Sink, Source}
import io.gearpump.cluster.ClusterConfig

/**
  *
  * This tests how different Materializers can be used together in an explicit way.
  * Usage: output/target/pack/bin/gear app -jar experiments/akkastream/target/scala.11/akkastream-2.11.5-0.6.2-SNAPSHOT-assembly.jar
  *
  */
object Test2 {

  def main(args: Array[String]): Unit = {

    println("running Test2...")
    implicit val system = ActorSystem("akka-test")
    val materializer = new GearpumpMaterializer(system)

    val echo = system.actorOf(Props(new Echo()))
    val source = GearSource.bridge[String, String]
    val sink = GearSink.bridge[String, String]

    val flow = Flow[String].filter(_.startsWith("red")).map("I want to order item: " + _)
    val (entry, exit) = flow.runWith(source, sink)(materializer)

    val actorMaterializer = ActorMaterializer()

    val externalSource = Source(List("red hat", "yellow sweater", "blue jack", "red apple", "green plant", "blue sky"))
    val externalSink = Sink.actorRef(echo, "COMPLETE")

    val graph = FlowGraph.closed() { implicit b =>
      import FlowGraph.Implicits._
      externalSource ~> Sink(entry)
      Source(exit) ~> externalSink
    }
    graph.run()(actorMaterializer)

    system.awaitTermination()
  }

  class Echo extends Actor {
    def receive: Receive = {
      case any: AnyRef =>
        println("Confirm received: " + any)
    }
  }
}