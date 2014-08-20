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

package org.apache.gearpump.examples.wordcount

import org.apache.gearpump.client.ClientContext
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{Graph, HashPartitioner}
import org.apache.gearpump.{AppDescription, TaskDescription}
import org.apache.gears.cluster.{Configs, Starter}

class WordCount  {
  def getApplication(splitNum : Int, sumNum : Int) : AppDescription = {
    val config = Configs.empty
    val partitioner = new HashPartitioner()
    val split = TaskDescription(classOf[Split], splitNum)
    val sum = TaskDescription(classOf[Sum], sumNum)
    val app = AppDescription("wordCount", config, Graph(split ~ partitioner ~> sum))
    app
  }
}

object WordCount extends App with Starter {
  case class Config(var ip: String = "", var split: Int = 1, var sum : Int = 1, var runseconds : Int = 60) extends super.Config

  def uuid = java.util.UUID.randomUUID.toString

  def usage = List("java org.apache.gearpump.examples.wordcount.WordCount-port <port> -ip <ip> -split <split number> -sum <sum number> -runseconds <how many seconds to run>")

  def start(): Unit = {
    val config: Config = Config()
    parse(args.toList, config)
    validate(config)
    val context = ClientContext()
    val kvServiceURL = s"http://${config.ip}:${config.port}/kv"

    Console.out.println("Init KV Service: " + kvServiceURL)

    context.init(kvServiceURL)
    val appId = context.submit(new WordCount().getApplication(config.split, config.sum))
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
        case "-split" :: split :: rest =>
          config.split = split.toInt
          doParse(rest)
        case "-sum" :: sum :: rest =>
          config.sum = sum.toInt
          doParse(rest)
        case "-runseconds":: runseconds :: rest =>
          config.runseconds = runseconds.toInt
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

  start()

}

