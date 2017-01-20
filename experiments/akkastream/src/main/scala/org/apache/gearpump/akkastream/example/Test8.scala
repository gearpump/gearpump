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
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Materializer}
import akka.stream.scaladsl._
import org.apache.gearpump.akkastream.GearpumpMaterializer
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.util.AkkaApp

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
 
/**
 * Stream example to find sum of elements
 */
object Test8 extends AkkaApp with ArgumentsParser {
  // scalastyle:off println
  override val options: Array[(String, CLIOption[Any])] = Array(
    "gearpump" -> CLIOption[Boolean]("<boolean>", required = false, defaultValue = Some(false))
  )

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    implicit val system = ActorSystem("Test8", akkaConf)
    implicit val materializer: ActorMaterializer = config.getBoolean("gearpump") match {
      case true =>
        GearpumpMaterializer()
      case false =>
        ActorMaterializer(
          ActorMaterializerSettings(system).withAutoFusing(false)
        )
    }
    implicit val ec = system.dispatcher

    // Source gives 1 to 100 elements
    val source: Source[Int, NotUsed] = Source(Stream.from(1).take(100))
    val sink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)(_ + _)

    val result: Future[Int] = source.runWith(sink)
    result.map(sum => {
      println(s"Sum of stream elements => $sum")
    })

    Await.result(system.whenTerminated, 60.minutes)
  }
  // scalastyle:on println
}
