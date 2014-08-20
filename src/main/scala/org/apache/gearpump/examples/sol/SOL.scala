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

package org.apache.gearpump.examples.sol

import org.apache.gearpump._
import org.apache.gearpump.client.ClientContext
import org.apache.gearpump.util.{ShufflePartitioner, Graph}
import org.apache.gearpump.util.Graph._
import org.apache.gears.cluster.{Starter, Configs}

class SOL  {
}

object SOL extends App with Starter {
  case class Config(var ip: String = "", var spout: Int = 1, var stages: Int = 2, var bolt : Int = 1, var bytesPerMessage : Int = 100, var runseconds : Int = 60) extends super.Config

  val usage = List(
    "java -port <port> -ip <ip> -spout <spout number> -bolt <bolt number> -runseconds <how many seconds to run> - -bytesPerMessage <bytes for each message>")

  def uuid = java.util.UUID.randomUUID.toString

  def start() = {
    val config = Config()
    parse(args.toList, config)
    validate(config)

    val context = ClientContext()
    val kvServiceURL = s"http://${config.ip}:${config.port}/kv"

    Console.out.println("Init KV Service: " + kvServiceURL)

    context.init(kvServiceURL)
    val appId = context.submit(getApplication(config.spout, config.bolt, config.bytesPerMessage, config.stages))
    System.out.println(s"We get application id: $appId")

    Thread.sleep(config.runseconds * 1000)

    System.out.println(s"Shutting down application $appId")

    context.shutdown(appId)
    context.destroy()
  }

  def parse(args: List[String], config: Config):Unit = {
    super.parse(args, config)
    def doParse(argument : List[String]) : Unit = {
      argument match {
        case Nil => Unit // true if everything processed successfully
        case "-ip" :: ip :: rest =>
          config.ip = ip
          doParse(rest)
        case "-spout" :: spout :: rest =>
          config.spout = spout.toInt
          doParse(rest)
        case "-stages"::stages::rest =>
          config.stages = stages.toInt
          doParse(rest)
        case "-bolt" :: bolt :: rest =>
          config.bolt = bolt.toInt
          doParse(rest)
        case "-runseconds":: runseconds :: rest =>
          config.runseconds = runseconds.toInt
          doParse(rest)
        case "-bytesPerMessage"::bytesPerMessage::rest =>
          config.bytesPerMessage = bytesPerMessage.toInt
          doParse(rest)
        case _ :: rest =>
          doParse(rest)
      }
    }
    doParse(args)
  }

  def validate(config: Config): Unit = {
    if(config.port == -1) {
      commandHelp()
      System.exit(-1)
    }
    if(config.ip.length == 0) {
      commandHelp()
      System.exit(-1)
    }
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

  start()
}
