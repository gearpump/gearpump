package org.apache.gearpump.examples.sol

/**
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

import akka.actor.Props
import org.apache.gearpump.client.ClientContext
import org.apache.gearpump.{AppDescription, HashPartitioner, StageDescription, TaskDescription}


class SOL  {
}

object SOL extends App{

  case class Config(ip : String = "", port : Int = -1, spout: Int = 1, bolt : Int = 1, runseconds : Int = 60, bytesPerMessage : Int = 100)

  start

  def start = {
    val config = parse(args.toList)

    val context = ClientContext()
    val kvServiceURL = s"http://${config.ip}:${config.port}/kv"

    Console.out.println("Init KV Service: " + kvServiceURL)

    context.init(kvServiceURL)
    val appId = context.submit(getApplication(config.spout, config.bolt, config.bytesPerMessage))
    System.out.println(s"We get application id: $appId")

    Thread.sleep(config.runseconds * 1000)

    System.out.println(s"Shutting down application $appId")

    context.shutdown(appId)
    context.destroy()
  }

  def commandHelp = {
    val command = List(
      "wordcount",
      "Start a wordcount",
      "java -cp <classpath> -ip <ip> -port <port> -spout <spout number> -bolt <bolt number> -runseconds <how many seconds to run> - -bytesPerMessage <bytes for each message>"
)

    Console.println("GearPump")
    Console.println("=============================\n")

    command.grouped(3).foreach{array =>
      array match {
        case command::description::example::_ =>
          Console.println(s"  [$command] $description")
          Console.println(s"  $example")
          Console.println("--")
      }
    }
  }

  def parse(args: List[String]) :Config = {
    var config = Config()

    def doParse(argument : List[String]) : Unit = {
      argument match {
        case Nil => true // true if everything processed successfully
        case "-port" :: port :: rest => {
          config = config.copy(port = port.toInt)
          doParse(rest)
        }
        case "-ip" :: ip :: rest => {
          config = config.copy(ip = ip)
          doParse(rest)
        }
        case "-spout" :: spout :: rest => {
          config = config.copy(spout = spout.toInt)
          doParse(rest)
        }
        case "-bolt" :: bolt :: rest => {
          config = config.copy(bolt = bolt.toInt)
          doParse(rest)
        }
        case "-runseconds":: runseconds :: rest => {
          config = config.copy(runseconds = runseconds.toInt)
          doParse(rest)
        }
        case "-bytesPerMessage"::bytesPerMessage::rest => {
          config = config.copy(bytesPerMessage = bytesPerMessage.toInt)
          doParse(rest)
        }
        case _ :: rest => {
          doParse(rest)
        }
      }
    }
    doParse(args)
    config
  }

  def getApplication(spoutNum : Int, boltNum : Int, bytesPerMessage : Int) : AppDescription = {
    val config = Map[String, Any]()
    val partitioner = new HashPartitioner()
    val spout = TaskDescription(Props(classOf[SOLSpout], bytesPerMessage), partitioner)
    val bolt = TaskDescription(Props(classOf[SOLBolt]), partitioner)
    val app = AppDescription("sol", config, Array(StageDescription(spout, spoutNum), StageDescription(bolt, boltNum)))
    app
  }
}
