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
package org.apache.gearpump.experiments.distributeservice

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{CLIOption, ArgumentsParser}
import org.apache.gearpump.experiments.distributeservice.DistServiceAppMaster.{DistributeFile, FileContainer, GetFileContainer}
import org.apache.gearpump.util.{FileServer, Constants}
import org.slf4j.{LoggerFactory, Logger}

import akka.pattern.ask
import scala.concurrent.Future
import scala.util.{Failure, Success}

object DistributeServiceClient extends App with ArgumentsParser{
  implicit val timeout = Constants.FUTURE_TIMEOUT
  import scala.concurrent.ExecutionContext.Implicits.global
  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    "appid" -> CLIOption[Int]("<the distributed shell appid>", required = true),
    "file" -> CLIOption[String]("<file path>", required = true)
  )

  val config = parse(args)
  val context = ClientContext(config.getString("master"))
  val appid = config.getInt("appid")
  val file = new File(config.getString("file"))
  val appMaster = context.resolveAppID(appid)
  (appMaster ? GetFileContainer).asInstanceOf[Future[FileContainer]].map { container =>
    val bytes = FileUtils.readFileToByteArray(file)
    val result = FileServer.newClient.save(container.url, bytes)
    result match {
      case Success(_) =>
        appMaster ! DistributeFile(container.url, file.getName)
        context.close()
      case Failure(ex) => throw ex
    }
  }
}
