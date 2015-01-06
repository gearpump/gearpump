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
package org.apache.gearpump.distributedshell

import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.experiments.cluster.AppMasterToExecutor.MsgToTask

import akka.pattern.ask
import org.apache.gearpump.util.Constants
import org.slf4j.{LoggerFactory, Logger}

object DistributedShellClient extends App with ArgumentsParser  {
  implicit val timeout = Constants.FUTURE_TIMEOUT
  import scala.concurrent.ExecutionContext.Implicits.global
  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    "appid" -> CLIOption[Int]("<the distributed shell appid>", required = true),
    "command" -> CLIOption[String]("<shell command>", required = true),
    "args" -> CLIOption[String]("<shell arguments>", required = true)
  )

  val config = parse(args)
  val context = ClientContext(config.getString("master"))
  val appid = config.getInt("appid")
  val command = config.getString("command")
  val arguments = config.getString("args")
  val appMaster = context.resolveAppID(appid)
  (appMaster ? MsgToTask(ShellCommand(command, arguments))).map { reslut =>
    LOG.info(s"Result: $reslut")
    context.close()
  }
}
