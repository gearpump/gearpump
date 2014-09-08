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

package org.apache.gearpump.cluster.main

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigValueFactory
import org.apache.gearpump.cluster.Configs
import org.apache.gearpump.util.{ActorSystemBooter, ActorUtil}
import org.slf4j.{Logger, LoggerFactory}


object Local extends App with ArgumentsParser {

  private val LOG: Logger = LoggerFactory.getLogger(Local.getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "port"-> CLIOption[Int]("<master port>", required = true),
    "sameprocess" -> CLIOption[Boolean]("", required = false, defaultValue = Some(false)),
    "workernum"-> CLIOption[Int]("<how many workers to start>", required = false, defaultValue = Some(4)))

  val config = parse(args)

  def start() = {
    local(config.getInt("port"), config.getInt("workernum"), config.exists("sameprocess"))
  }

  def local(port : Int, workerCount : Int, sameProcess : Boolean) : Unit = {
    if (sameProcess) {
      System.setProperty("LOCAL", "true")
    }

    val system = ActorSystem(Configs.MASTER, Configs.MASTER_CONFIG.withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(port)))
    system.actorOf(Props[org.apache.gearpump.cluster.Master], Configs.MASTER)
    val masterPath = ActorUtil.getSystemPath(system) + s"/user/${Configs.MASTER}"

    LOG.info(s"master is started at $masterPath...")

    //We are free
    0.until(workerCount).foreach(id => ActorSystemBooter.create(Configs.WORKER_CONFIG).boot(classOf[org.apache.gearpump.cluster.Worker].getSimpleName + id , masterPath))
  }
  start()
}
