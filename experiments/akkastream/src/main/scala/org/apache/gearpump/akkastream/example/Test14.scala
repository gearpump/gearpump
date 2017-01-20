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

import java.io.File

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import org.apache.gearpump.akkastream.GearpumpMaterializer
import org.apache.gearpump.cluster.main.ArgumentsParser
import org.apache.gearpump.util.AkkaApp

import scala.concurrent._
import scala.concurrent.duration._

object Test14 extends AkkaApp with ArgumentsParser {
  // scalastyle:off println
  override def main(akkaConf: Config, args: Array[String]): Unit = {
    implicit val system = ActorSystem("Test14", akkaConf)
    implicit val materializer = GearpumpMaterializer()

    def lineSink(filename: String): Sink[String, Future[IOResult]] = {
      Flow[String]
        .alsoTo(Sink.foreach(s => println(s"$filename: $s")))
        .map(s => ByteString(s + "\n"))
        .toMat(FileIO.toPath(new File(filename).toPath))(Keep.right)
    }

    val source: Source[Int, NotUsed] = Source(1 to 100)
    val factorials: Source[BigInt, NotUsed] = source.scan(BigInt(1))((acc, next) => acc * next)
    val sink1 = lineSink("factorial1.txt")
    val sink2 = lineSink("factorial2.txt")
    val slowSink2 = Flow[String].via(
      Flow[String].throttle(1, 1.second, 1, ThrottleMode.shaping)
    ).toMat(sink2)(Keep.right)
    val bufferedSink2 = Flow[String].buffer(50, OverflowStrategy.backpressure).via(
      Flow[String].throttle(1, 1.second, 1, ThrottleMode.shaping)
    ).toMat(sink2)(Keep.right)

    val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val bcast = b.add(Broadcast[String](2))
      factorials.map(_.toString) ~> bcast.in
      bcast.out(0) ~> sink1
      bcast.out(1) ~> bufferedSink2
      ClosedShape
    })

    g.run()

    Await.result(system.whenTerminated, 60.minutes)
  }
  // scalastyle:on println
}
