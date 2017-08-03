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

import akka.NotUsed
import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, ClosedShape}
import akka.stream.scaladsl._
import org.apache.gearpump.akkastream.GearpumpMaterializer
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.util.AkkaApp

import scala.concurrent.Await
import scala.concurrent.duration._
 
/**
 * Stream example showing Broadcast
 */
object Test9 extends AkkaApp with ArgumentsParser {
  // scalastyle:off println
  override val options: Array[(String, CLIOption[Any])] = Array(
    "gearpump" -> CLIOption[Boolean]("<boolean>", required = false, defaultValue = Some(false))
  )

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    implicit val system = ActorSystem("Test9", akkaConf)
    implicit val materializer: ActorMaterializer = config.getBoolean("gearpump") match {
      case true =>
        GearpumpMaterializer()
      case false =>
        ActorMaterializer(
          ActorMaterializerSettings(system).withAutoFusing(false)
        )
    }
    implicit val ec = system.dispatcher

    val sinkActor = system.actorOf(Props(new SinkActor()))
    val source = Source((1 to 5))
    val sink = Sink.actorRef(sinkActor, "COMPLETE")
    val flowA: Flow[Int, Int, NotUsed] = Flow[Int].map {
      x => println(s"processing broadcasted element : $x in flowA"); x
    }
    val flowB: Flow[Int, Int, NotUsed] = Flow[Int].map {
      x => println(s"processing broadcasted element : $x in flowB"); x
    }

    val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val broadcast = b.add(Broadcast[Int](2))
      val merge = b.add(Merge[Int](2))
      source ~> broadcast
      broadcast ~> flowA ~> merge
      broadcast ~> flowB ~> merge
      merge ~> sink
      ClosedShape
    })

    graph.run()

    Await.result(system.whenTerminated, 60.minutes)
  }

  class SinkActor extends Actor {
    def receive: Receive = {
      case any: AnyRef =>
        println("Confirm received: " + any)
    }
  }
  // scalastyle:on println
}
