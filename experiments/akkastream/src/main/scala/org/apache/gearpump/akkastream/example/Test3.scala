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
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.apache.gearpump.akkastream.GearpumpMaterializer
import org.apache.gearpump.akkastream.scaladsl.GearSource
import akka.stream.scaladsl.Sink
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.streaming.dsl.scalaapi.CollectionDataSource
import org.apache.gearpump.util.AkkaApp

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * read from remote and write to local
 */
object Test3 extends AkkaApp with ArgumentsParser {
  // scalastyle:off println
  override val options: Array[(String, CLIOption[Any])] = Array(
    "gearpump" -> CLIOption[Boolean]("<boolean>", required = false, defaultValue = Some(false))
  )

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    implicit val system = ActorSystem("Test3", akkaConf)
    implicit val materializer: ActorMaterializer = config.getBoolean("gearpump") match {
      case true =>
        GearpumpMaterializer()
      case false =>
        ActorMaterializer(
          ActorMaterializerSettings(system).withAutoFusing(false)
        )
    }
    val echo = system.actorOf(Props(new Echo()))
    val sink = Sink.actorRef(echo, "COMPLETE")
    val sourceData = new CollectionDataSource(
      List("red hat", "yellow sweater", "blue jack", "red apple", "green plant", "blue sky"))
    val source = GearSource.from[String](sourceData)
    source.filter(_.startsWith("red")).map("I want to order item: " + _).runWith(sink)

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
