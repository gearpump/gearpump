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

import akka.actor.ActorSystem
import akka.stream.gearpump.GearpumpMaterializer
import akka.stream.gearpump.scaladsl.GearSink
import akka.stream.scaladsl.Source
import io.gearpump.cluster.ClusterConfig
import io.gearpump.streaming.dsl.LoggerSink

/**
  * read from local and write to remote
  * Usage: output/target/pack/bin/gear app -jar experiments/akkastream/target/scala.11/akkastream-2.11.5-0.6.2-SNAPSHOT-assembly.jar
  */
object Test4 {

  def main(args: Array[String]): Unit = {

    println("running Test...")

    implicit val system = ActorSystem("akka-test")
    implicit val materializer = new GearpumpMaterializer(system)

    val sink = GearSink.to(new LoggerSink[String])
    val source = Source(List("red hat", "yellow sweater", "blue jack", "red apple", "green plant", "blue sky"))
    source.filter(_.startsWith("red")).map("I want to order item: " + _).runWith(sink)

    system.awaitTermination()
  }
}