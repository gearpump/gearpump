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

class LocalCluster {
  private var system : ActorSystem = null
  private val LOG: Logger = LoggerFactory.getLogger(classOf[LocalCluster])

  def start(kvService : String, workerCount: Int) = {
    SimpleKVService.init(kvService)
    system = ActorSystem("cluster", Configs.SYSTEM_DEFAULT_CONFIG)

    system.actorOf(Props[Master], "master")
    LOG.info("master is started...")

    val masterPath = ActorUtil.getSystemPath(system) + "/user/master"
    SimpleKVService.set("master", masterPath)

    //We are free
    0.until(workerCount).foreach(id => ActorSystemBooter.create.boot(classOf[Worker].getSimpleName + id , masterPath))
    this
  }

  def awaitTermination() = {
    system.awaitTermination()
  }
}

object LocalCluster {
  def create = new LocalCluster
}

object Local extends App with Starter {
  def uuid = java.util.UUID.randomUUID.toString

  def usage = List(
    "Start a local cluster",
    "java org.apache.gears.cluster.Local -port <port> [-sameprocess <true|false>] [-workernum <number of workers>]")

  def validate(config: Config): Unit = {
    if(config.port == -1) {
      commandHelp()
      System.exit(-1)
    }
  }

  def start() = {
    val config = parse(args.toList)
    Console.println(s"Configuration after parse $config")
    validate(config)
    local(config.port, config.workerCount, config.sameProcess)
  }

  def local(port : Int, workerCount : Int, sameProcess : Boolean) : Unit = {
    if (sameProcess) {
      System.setProperty("LOCAL", "true")
    }
    SimpleKVService.create.start(port)
    val url = s"http://127.0.0.1:$port/kv"
    LocalCluster.create.start(url, workerCount).awaitTermination()
  }

  start()
}