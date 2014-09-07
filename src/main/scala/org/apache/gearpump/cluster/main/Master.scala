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
import org.apache.gearpump.cluster.{Configs, Master => MasterClass}
import org.apache.gearpump.util.ActorUtil
import org.slf4j.{Logger, LoggerFactory}

object Master extends App with ArgumentsParser {
  private val LOG: Logger = LoggerFactory.getLogger(Master.getClass)

  val options: Array[(String, CLIOption[Any])] = Array("port"->CLIOption("<master port>",required = true))

  val config = parse(args)

  def start() = {
    master(config.getInt("port"))
  }

  def master(port : Int): Unit = {

    val system = ActorSystem(Configs.MASTER, Configs.MASTER_CONFIG.withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(port)))

    system.actorOf(Props[MasterClass], Configs.MASTER)
    val masterPath = ActorUtil.getSystemPath(system) + s"/user/${Configs.MASTER}"
    LOG.info(s"master is started at $masterPath")
    system.awaitTermination()
  }

  start()

}