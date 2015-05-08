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
package gearpump.examples.distributedshell

import gearpump.cluster.main.ArgumentsParser
import gearpump.cluster.client.ClientContext
import gearpump.cluster.main.{CLIOption, ArgumentsParser}
import DistShellAppMaster.ShellCommand

import akka.pattern.ask
import gearpump.util.Constants
import org.slf4j.{LoggerFactory, Logger}

object DistributedShellClient extends App with ArgumentsParser  {
  implicit val timeout = Constants.FUTURE_TIMEOUT
  import scala.concurrent.ExecutionContext.Implicits.global
  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "appid" -> CLIOption[Int]("<the distributed shell appid>", required = true),
    "command" -> CLIOption[String]("<shell command>", required = true)
  )

  val config = parse(args)
  val context = ClientContext()
  val appid = config.getInt("appid")
  val command = config.getString("command")
  val appMaster = context.resolveAppID(appid)
  LOG.info(s"Resolved appMaster $appid address ${appMaster.path.toString}, sending command $command")
  (appMaster ? ShellCommand(command)).map { result =>
    LOG.info(s"Result: $result")
    context.close()
  }
}