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

import org.apache.gearpump.cluster.Configs
import org.apache.gearpump.util.ActorSystemBooter
import org.slf4j.{Logger, LoggerFactory}

object Worker extends App with ArgumentsParser {
  val LOG : Logger = LoggerFactory.getLogger(Worker.getClass)

  def uuid = java.util.UUID.randomUUID.toString

  val options:Array[(String, CLIOptionType)] = Array(
    "ip"-> CLIOption("<master ip>", required = true, defaultValue = "127.0.0.1"),
    "port"-> CLIOption("<master port>", required = true, defaultValue = 8092))

  def start() = {
    worker(parse(args).getString("ip"), parse(args).getInt("port"))
  }

  def worker(ip : String, port : Int): Unit = {
    val masterURL = s"akka.tcp://${Configs.MASTER}@$ip:$port/user/${Configs.MASTER}"
    ActorSystemBooter.create(Configs.WORKER_CONFIG).boot(uuid, masterURL).awaitTermination
  }

  start()
}
