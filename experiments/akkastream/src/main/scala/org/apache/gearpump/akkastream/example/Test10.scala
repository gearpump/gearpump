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
import akka.stream.{ClosedShape, ThrottleMode}
import org.apache.gearpump.akkastream.GearpumpMaterializer
import org.apache.gearpump.cluster.main.ArgumentsParser
import org.apache.gearpump.util.AkkaApp

import scala.concurrent.Await
import scala.concurrent.duration._
 
/**
 * Stream example showing Conflate, Throttle
 */
object Test10 extends AkkaApp with ArgumentsParser {

  // scalastyle:off println
  override def main(akkaConf: Config, args: Array[String]): Unit = {
    import akka.actor.ActorSystem
    import akka.stream.scaladsl._

    implicit val system = ActorSystem("Test10", akkaConfig)
    implicit val materializer = GearpumpMaterializer()
    implicit val ec = system.dispatcher

    // Conflate[A] - (2 inputs, 1 output) concatenates two streams
    // (first consumes one, then the second one)
    def stream(x: String) = Stream.continually(x)

    val sourceA = Source(stream("A"))
    val sourceB = Source(stream("B"))

    val throttler: Flow[String, String, NotUsed] =
      Flow[String].throttle(1, 1.second, 1, ThrottleMode.Shaping)
    val conflateFlow: Flow[String, String, NotUsed] =
      Flow[String].conflate((x: String, y: String) => x: String)
      ((acc: String, x: String) => s"$acc::$x")

    val printFlow: Flow[(String, String), String, NotUsed] =
      Flow[(String, String)].map {
        x =>
          println(s" lengths are : ${x._1.length} and ${x._2.length}  ;  ${x._1} zip ${x._2}")
          x.toString
      }

    val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val zipping = b.add(Zip[String, String]())

      sourceA ~> throttler ~> zipping.in0
      sourceB ~> conflateFlow ~> zipping.in1

      zipping.out ~> printFlow ~> Sink.ignore

      ClosedShape
    })

    graph.run()

    Await.result(system.whenTerminated, 60.minutes)
  }
  // scalastyle:on println
}
