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

package org.apache.gears.cluster

import akka.actor.{ActorSystem, Props}
import org.apache.gearpump.ActorUtil
import org.apache.gearpump.kvservice.SimpleKVService
import org.apache.gearpump.util.ActorSystemBooter
import org.slf4j.{Logger, LoggerFactory}

/**
 * Command line tool to start master, worker, client, and local mode
 */
object Starter extends App {

  private val LOG: Logger = LoggerFactory.getLogger(Starter.getClass)

  case class Config(local : Boolean = false, port : Int = -1,
                    sameProcess : Boolean = false, role : String = "",
                    workerCount : Int = 1, ip : String = "", arguments : Array[String] = null)

  start

  def start = {
    val config = parse(args.toList)

    Console.println(s"Configuration after parse $config")

    if (config.local) {
      local(config.port, config.workerCount, config.sameProcess)
    } else {
      val role = config.role
      val className = ""
      role match {
        case "master" => master(config.port)
        case "worker" => worker(config.ip, config.port)
        case _ => commandHelp
      }
    }
  }

  def commandHelp = {
    val command = List(
      "local",
      "Start a local cluster",
      "java -cp <classpath> local -port <port> -sameprocess <true|false> -workernum <number of workers>",

      "master",
      "Start Master Node, the Master node must be started first",
      "java -cp <CLASSPATH> master -port <port>",

      "worker",
      "Start Worker Node, It need to be started after Master",
      "java -cp <classpath> worker -ip <master ip> -port <master port>")

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
        case Nil => Unit // true if everything processed successfully
        case "local" :: rest => {
          config = config.copy(local = true)
          doParse(rest)
        }
        case "-port" :: port :: rest => {
          config = config.copy(port = port.toInt)
          doParse(rest)
        }
        case "-sameprocess" :: sameprocess :: rest  => {
          config = config.copy(sameProcess = sameprocess.toBoolean)
          doParse(rest)
        }
        case "master" :: rest => {
          config = config.copy(role = "master")
          doParse(rest)
        }
        case "worker" :: rest => {
          config = config.copy(role = "worker")
          doParse(rest)
        }
        case "-workernum" :: worker :: rest => {
          config = config.copy(workerCount = worker.toInt)
          doParse(rest)
        }
        case "-ip" :: ip :: rest => {
          config = config.copy(ip = ip)
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


  def local(port : Int, workerCount : Int, sameProcess : Boolean) : Unit = {
    if (sameProcess) {
      System.setProperty("LOCAL", "true")
    }
    SimpleKVService.create.start(port)
    val url = s"http://127.0.0.1:$port/kv"
    LocalCluster.create.start(url, workerCount).awaitTermination
  }

  def master(port : Int): Unit = {
    val url = s"http://127.0.0.1:$port/kv"
    Console.out.println(s"kv service url: $url")

    SimpleKVService.create.start(port)

    SimpleKVService.init(url)
    val system = ActorSystem("cluster", Configs.SYSTEM_DEFAULT_CONFIG)

    val master = system.actorOf(Props[Master], "master")
    LOG.info("master is started...")

    val masterPath = ActorUtil.getSystemPath(system) + "/user/master"
    SimpleKVService.set("master", masterPath)
    system.awaitTermination()
  }

  def worker(ip : String, port : Int): Unit = {
    val kvService = s"http://$ip:$port/kv"
    SimpleKVService.init(kvService)
    val master = SimpleKVService.get("master")
    ActorSystemBooter.create.boot(uuid, master).awaitTermination
  }

  def uuid = java.util.UUID.randomUUID.toString
}