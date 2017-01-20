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
import akka.stream.ClosedShape
import org.apache.gearpump.akkastream.GearpumpMaterializer
import org.apache.gearpump.cluster.main.ArgumentsParser
import org.apache.gearpump.util.AkkaApp

import scala.concurrent.Await
import scala.concurrent.duration._
 
/**
 * Stream example showing Broadcast and Merge
 */
object Test11 extends AkkaApp with ArgumentsParser {
  // scalastyle:off println
  override def main(akkaConf: Config, args: Array[String]): Unit = {
    import akka.actor.ActorSystem
    import akka.stream.scaladsl._

    implicit val system = ActorSystem("Test11", akkaConfig)
    implicit val materializer = GearpumpMaterializer()
    // implicit val materializer =
    //   ActorMaterializer(ActorMaterializerSettings(system).withAutoFusing(false))
    implicit val ec = system.dispatcher

    val g = RunnableGraph.fromGraph(GraphDSL.create() {
      implicit builder: GraphDSL.Builder[NotUsed] =>

      import GraphDSL.Implicits._
      val in = Source(1 to 10)
      val output: (Any) => Unit = any => {
        val s = s"**** $any"
        println(s)
      }
      val out = Sink.foreach(output)

      val broadcast = builder.add(Broadcast[Int](2))
      val merge = builder.add(Merge[Int](2))

      val f1, f2, f3, f4 = Flow[Int].map(_ + 10)

      in ~> f1 ~> broadcast ~> f2 ~> merge ~> f3 ~> out
      broadcast ~> f4 ~> merge

      ClosedShape
    })

    g.run()

    Await.result(system.whenTerminated, 60.minutes)
  }
  // scalastyle:on println
}
