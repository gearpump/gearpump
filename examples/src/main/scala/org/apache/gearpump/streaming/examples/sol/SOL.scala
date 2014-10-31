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

package org.apache.gearpump.streaming.examples.sol

import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.partitioner.{Partitioner, ShufflePartitioner}
import org.apache.gearpump.streaming.client.ClientContext
import org.apache.gearpump.streaming.{AppDescription, TaskDescription, _}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{Configs, Graph}
object SOL extends App with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    "spout"-> CLIOption[Int]("<spout number>", required = false, defaultValue = Some(2)),
    "bolt"-> CLIOption[Int]("<bolt number>", required = false, defaultValue = Some(2)),
    "runseconds" -> CLIOption[Int]("<run seconds>", required = false, defaultValue = Some(60)),
    "bytesPerMessage" -> CLIOption[Int]("<size of each message>", required = false, defaultValue = Some(100)),
    "stages"-> CLIOption[Int]("<how many stages to run>", required = false, defaultValue = Some(2)),
    "jar"-> CLIOption[String]("<jar file holding TaskActors>", required = false, defaultValue = Some("examples/target/gearpump-examples-0.1.jar")))

      start()

  def start(): Unit = {

    val config = parse(args)

    val masters = config.getString("master")
    val spout = config.getInt("spout")
    val bolt = config.getInt("bolt")
    val bytesPerMessage = config.getInt("bytesPerMessage")
    val stages = config.getInt("stages")
    val runseconds = config.getInt("runseconds")
    val jar = config.getString("jar")

    Console.out.println("Master URL: " + masters)

    getApplication(spout, bolt, bytesPerMessage, stages, jar).map(application => {
      val context = ClientContext(masters)

      val appId = context.submit(application)
      System.out.println(s"We get application id: $appId")

      Thread.sleep(runseconds * 1000)

      System.out.println(s"Shutting down application $appId")

      context.shutdown(appId)
      context.destroy()
    })
    System.out.println("Failed to load application")

  }

  def getApplication(spoutNum : Int, boltNum : Int, bytesPerMessage : Int, stages : Int, jarName: String) : Option[AppDescription] = {
    val config = Configs.empty.withValue(SOLSpout.BYTES_PER_MESSAGE, bytesPerMessage)
    val partitioner = new ShufflePartitioner()
    val jar = new java.io.File(jarName)
    val app: Option[AppDescription] = jar.map(jar => {
      val spout = TaskDescription(classOf[SOLSpout].getCanonicalName, spoutNum, jar)
      val bolt = TaskDescription(classOf[SOLBolt].getCanonicalName, boltNum, jar)
      var computation: Any = spout ~ partitioner ~> bolt
      computation = 0.until(stages - 2).foldLeft(computation) { (c, id) =>
        c ~ partitioner ~> bolt.copy()
      }
      val dag = Graph[TaskDescription, Partitioner](computation)
      val app = AppDescription("sol", config, dag)
      app
    })
    app
  }
}
