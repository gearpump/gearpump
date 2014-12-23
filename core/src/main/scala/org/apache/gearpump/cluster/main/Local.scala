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

import akka.actor.{Props, ActorSystem}
import com.typesafe.config.ConfigValueFactory
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{ActorUtil, Configs}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object Local extends App with ArgumentsParser {

  private val LOG: Logger = LoggerFactory.getLogger(Local.getClass)

  override val options: Array[(String, CLIOption[Any])] =
    Array("ip"->CLIOption[String]("<master ip address>",required = false, defaultValue = Some("127.0.0.1")),
          "port"->CLIOption("<master port>",required = true),
          "sameprocess" -> CLIOption[Boolean]("", required = false, defaultValue = Some(false)),
          "workernum"-> CLIOption[Int]("<how many workers to start>", required = false, defaultValue = Some(2)))

  val config = parse(args)

  def start() = {
    local(config.getString("ip"),  config.getInt("port"), config.getInt("workernum"), config.getBoolean("sameprocess"))
  }

  def local(ip : String, port : Int, workerCount : Int, sameProcess : Boolean) : Unit = {
    if (sameProcess) {
      LOG.info("Starting local in same process")
      System.setProperty("LOCAL", "true")
    }

    implicit val system = ActorSystem(MASTER, Configs.loadMasterConfig().
      withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(port)).
      withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(ip)).
      withValue("gearpump.cluster.masters",  ConfigValueFactory.fromAnyRef(List(s"$ip:$port").asJava))
    )

    val master = system.actorOf(Props[org.apache.gearpump.cluster.Master], MASTER)
    val masterPath = ActorUtil.getSystemPath(system) + s"/user/$MASTER"
    LOG.info(s"master is started at $masterPath...")

    0.until(workerCount).foreach { id =>
      system.actorOf(Props(classOf[org.apache.gearpump.cluster.Worker], master), classOf[org.apache.gearpump.cluster.Worker].getSimpleName + id)
    }
  }
  start()
}
