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

import org.apache.gearpump._
import org.apache.gearpump.partitioner.{ShufflePartitioner, Partitioner}
import org.apache.gearpump.streaming.client.ClientContext
import org.apache.gearpump.streaming.{TaskDescription, AppDescription}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.Graph
import org.apache.gearpump.cluster.Configs
import org.apache.gearpump.cluster.main.ArgumentsParser

object SOL extends App with ArgumentsParser {

  override val options = Array("ip" -> "master ip", "port" -> "master port",
    "spout"->"spout number", "bolt"->"bolt number", "runseconds" -> "run seconds", "bytesPerMessage" -> "sze of each message", "stages"->"how many stages to run")

  start

  def start = {
    val config = parse(args)

    val ip = config.getString("ip")
    val port = config.getInt("port")
    val spout = config.getInt("spout")
    val bolt = config.getInt("bolt")
    val bytesPerMessage = config.getInt("bytesPerMessage")
    val stages = config.getInt("stages")
    val runseconds = config.getInt("runseconds")

    val masterURL = s"akka.tcp://${Configs.MASTER}@$ip:$port/user/${Configs.MASTER}"
    Console.out.println("Master URL: " + masterURL)
    val context = ClientContext(masterURL)

    val appId = context.submit(getApplication(spout, bolt, bytesPerMessage, stages))
    System.out.println(s"We get application id: $appId")

    Thread.sleep(runseconds * 1000)

    System.out.println(s"Shutting down application $appId")

    context.shutdown(appId)
    context.destroy()
  }

  def getApplication(spoutNum : Int, boltNum : Int, bytesPerMessage : Int, stages : Int) : AppDescription = {
    val config = Configs.empty.withValue(SOLSpout.BYTES_PER_MESSAGE, bytesPerMessage)
    val partitioner = new ShufflePartitioner()
    val spout = TaskDescription(classOf[SOLSpout], spoutNum)
    val bolt = TaskDescription(classOf[SOLBolt], boltNum)

    var computation : Any = spout ~ partitioner ~> bolt
    computation = 0.until(stages - 2).foldLeft(computation) { (c, id) =>
      c ~ partitioner ~> bolt.copy()
    }

    val dag = Graph[TaskDescription, Partitioner](computation)
    val app = AppDescription("sol", config, dag)
    app
  }
}
