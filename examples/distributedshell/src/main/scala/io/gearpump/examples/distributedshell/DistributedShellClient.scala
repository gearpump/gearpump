/*
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gearpump.examples.distributedshell

import akka.pattern.ask
import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import io.gearpump.examples.distributedshell.DistShellAppMaster.ShellCommand
import io.gearpump.util.{AkkaApp, Constants}
import java.util.concurrent.TimeUnit
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/** Client to DistributedShell to input "shell command" */
object DistributedShellClient extends AkkaApp with ArgumentsParser {
  implicit val timeout = Constants.FUTURE_TIMEOUT
  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "appid" -> CLIOption[Int]("<the distributed shell appid>", required = true),
    "command" -> CLIOption[String]("<shell command>", required = true)
  )

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    val context = ClientContext(akkaConf)
    implicit val system = context.system
    implicit val dispatcher = system.dispatcher
    val appid = config.getInt("appid")
    val command = config.getString("command")
    val appMaster = context.resolveAppID(appid)
    LOG.info(s"Resolved appMaster $appid address $appMaster, sending command $command")
    val future = (appMaster ? ShellCommand(command)).map { result =>
      LOG.info(s"Result: \n$result")
      context.close()
    }
    Await.ready(future, Duration(60, TimeUnit.SECONDS))
  }
}