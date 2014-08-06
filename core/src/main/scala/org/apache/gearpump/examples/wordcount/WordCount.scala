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

package org.apache.gearpump.app.examples.wordcount

import org.apache.gearpump.client.ClientContext
import org.apache.gearpump.util.{HashPartitioner, Graph}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.{AppDescription, TaskDescription}
import org.apache.gears.cluster.Configs

class WordCount  {
}

object WordCount extends App{

  case class Config(ip : String = "", port : Int = -1, split: Int = 1, sum : Int = 1, runseconds : Int = 60)

  start

  def start = {
    val config = parse(args.toList)

    val context = ClientContext()
    val kvServiceURL = s"http://${config.ip}:${config.port}/kv"

    Console.out.println("Init KV Service: " + kvServiceURL)

    context.init(kvServiceURL)
    val appId = context.submit(getApplication(config.split, config.sum))
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
      "java -cp <classpath> -ip <ip> -port <port> -split <split number> -sum <sum number> -runseconds <how many seconds to run>"
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
        case "-split" :: split :: rest => {
          config = config.copy(split = split.toInt)
          doParse(rest)
        }
        case "-sum" :: sum :: rest => {
          config = config.copy(sum = sum.toInt)
          doParse(rest)
        }
        case "-runseconds":: runseconds :: rest => {
          config = config.copy(runseconds = runseconds.toInt)
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

  def getApplication(splitNum : Int, sumNum : Int) : AppDescription = {
    val config = Configs.empty
    val partitioner = new HashPartitioner()
    val split = TaskDescription(classOf[Split], splitNum)
    val sum = TaskDescription(classOf[Sum], sumNum)
    val app = AppDescription("wordCount", config, Graph(split ~ partitioner ~> sum))
    app
  }
}
