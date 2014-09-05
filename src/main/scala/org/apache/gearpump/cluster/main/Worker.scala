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
import org.apache.gearpump.cluster.{Configs, Worker}
import org.apache.gearpump.util.MasterProxy
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object Worker extends App with ArgumentsParser {
  val LOG : Logger = LoggerFactory.getLogger(Worker.getClass)

  def uuid = java.util.UUID.randomUUID.toString

  val options = Array.empty[(String, String)]

  def start() = {
    worker()
  }

  def worker(): Unit = {
    val config = Configs.WORKER_CONFIG

    val id = uuid
    val system = ActorSystem(id, config)

    val masterAddress = config.getStringList("gearpump.cluster.masters").asScala
    val masterProxy = system.actorOf(Props(classOf[MasterProxy], masterAddress), Configs.MASTER_PROXY)

    val worker = system.actorOf(Props(classOf[Worker], masterProxy), classOf[Worker].getSimpleName + id)

    system.awaitTermination
  }

  start()
}
