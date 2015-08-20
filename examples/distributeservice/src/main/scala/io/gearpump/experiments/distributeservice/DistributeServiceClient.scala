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
package io.gearpump.experiments.distributeservice

import java.io.File
import org.apache.commons.io.FileUtils
import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.{CLIOption, ArgumentsParser}
import DistServiceAppMaster.{InstallService, FileContainer, GetFileContainer}
import io.gearpump.util.{AkkaApp, LogUtil, FileServer, Constants}
import org.slf4j.{LoggerFactory, Logger}

import akka.pattern.ask
import scala.concurrent.Future
import scala.util.{Failure, Success}

object DistributeServiceClient extends AkkaApp with ArgumentsParser{
  implicit val timeout = Constants.FUTURE_TIMEOUT

  override val options: Array[(String, CLIOption[Any])] = Array(
    "appid" -> CLIOption[Int]("<the distributed shell appid>", required = true),
    "file" -> CLIOption[String]("<service zip file path>", required = true),
    "script" -> CLIOption[String]("<file path of service script that will be installed to /etc/init.d>", required = true),
    "serviceName" -> CLIOption[String]("<service name>", required = true),
    "target" -> CLIOption[String]("<target path on each machine>", required = true)
  )

  override def help : Unit = {
    super.help
    Console.println(s"-D<name>=<value> set a property to the service")
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(filterCustomOptions(args))
    val context = ClientContext(akkaConf)
    implicit val system = context.system
    implicit val dispatcher = system.dispatcher
    val appid = config.getInt("appid")
    val zipFile = new File(config.getString("file"))
    val script = new File(config.getString("script"))
    val serviceName = config.getString("serviceName")
    val appMaster = context.resolveAppID(appid)
    (appMaster ? GetFileContainer).asInstanceOf[Future[FileContainer]].map { container =>
      val bytes = FileUtils.readFileToByteArray(zipFile)
      val result = FileServer.newClient.save(container.url, bytes)
      result match {
        case Success(_) =>
          appMaster ! InstallService(container.url, zipFile.getName, config.getString("target"),
            FileUtils.readFileToByteArray(script), serviceName, parseServiceConfig(args))
          context.close()
        case Failure(ex) => throw ex
      }
    }
  }

  private def filterCustomOptions(args: Array[String]): Array[String] = {
    args.filter(!_.startsWith("-D"))
  }

  private def parseServiceConfig(args: Array[String]): Map[String, Any] = {
    val result = Map.empty[String, Any]
    args.foldLeft(result) { (result, argument) =>
      if(argument.startsWith("-D") && argument.contains("=")) {
        val fixedKV = argument.substring(2).split("=")
        result + (fixedKV(0) -> fixedKV(1))
      } else {
        result
      }
    }
  }
}
