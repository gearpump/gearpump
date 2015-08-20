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

import java.io.{FileWriter, File}
import java.net.InetAddress

import akka.actor.Actor
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.text.StrSubstitutor
import io.gearpump.cluster.{UserConfig, ExecutorContext}
import DistServiceAppMaster.InstallService
import io.gearpump.util.{ActorUtil, FileServer, LogUtil}
import org.slf4j.Logger

import scala.io.Source
import scala.sys.process._
import collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class DistServiceExecutor(executorContext: ExecutorContext, userConf : UserConfig) extends Actor {
  import executorContext._
  private val LOG: Logger = LogUtil.getLogger(getClass, executor = executorId, app = appId)

  override def receive: Receive = {
    case InstallService(url, zipFileName, targetPath, scriptData, serviceName, serviceSettings) =>
      LOG.info(s"Executor $executorId receive command to install service $serviceName to $targetPath")
      unzipFile(url, zipFileName, targetPath)
      installService(scriptData, serviceName, serviceSettings)
  }

  private def unzipFile(url: String, zipFileName: String, targetPath: String) = {
    val zipFile = File.createTempFile(System.currentTimeMillis().toString, zipFileName)
    val dir = new File(targetPath)
    if(dir.exists()) {
      FileUtils.forceDelete(dir)
    }
    val bytes = FileServer.newClient.get(url).get
    FileUtils.writeByteArrayToFile(zipFile, bytes)
    val result = Try(s"unzip ${zipFile.getAbsolutePath} -d $targetPath" !!)
    result match {
      case Success(msg) => LOG.info(s"Executor $executorId unzip file to $targetPath")
      case Failure(ex) =>  throw ex
    }
  }

  private def installService(scriptData: Array[Byte], serviceName: String, serviceSettings: Map[String, Any]) = {
    val tempFile = File.createTempFile("gearpump", serviceName)
    FileUtils.writeByteArrayToFile(tempFile, scriptData)
    val script = new File("/etc/init.d", serviceName)
    writeFileWithEnvVariables(tempFile, script, serviceSettings ++ getEnvSettings)
    val result = Try(s"chkconfig --add $serviceName" !!)
    result match {
      case Success(msg) => LOG.info(s"Executor install service $serviceName successfully!")
      case Failure(ex) => throw ex
    }
  }

  private def getEnvSettings : Map[String, Any] = {
    Map("workerId" -> worker,
      "localhost" -> ActorUtil.getSystemAddress(context.system).host.get,
      "hostname" -> InetAddress.getLocalHost.getHostName)
  }

  private def writeFileWithEnvVariables(source: File, target: File, envs: Map[String, Any]) = {
    val writer = new FileWriter(target)
    val sub = new StrSubstitutor(mapAsJavaMap(envs))
    sub.setEnableSubstitutionInVariables(true)
    Source.fromFile(source).getLines().foreach(line => writer.write(sub.replace(line) + "\r\n"))
    writer.close()
  }
}
