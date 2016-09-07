/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.gearpump.services

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.nio.file.StandardOpenOption.{APPEND, WRITE}
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success}

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.ParameterDirectives.ParamMagnet
import akka.http.scaladsl.unmarshalling.Unmarshaller._
import akka.stream.Materializer
import com.typesafe.config.Config

import org.apache.gearpump.cluster.AppMasterToMaster.{GetAllWorkers, GetMasterData, GetWorkerData, MasterData, WorkerData}
import org.apache.gearpump.cluster.ClientToMaster.{QueryHistoryMetrics, QueryMasterConfig, ReadOption}
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMastersData, AppMastersDataRequest, WorkerList}
import org.apache.gearpump.cluster.MasterToClient.{HistoryMetrics, MasterConfig, SubmitApplicationResultValue}
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.worker.WorkerSummary
import org.apache.gearpump.cluster.{ClusterConfig, UserConfig}
import org.apache.gearpump.jarstore.{JarStoreClient, FileDirective, JarStoreServer}
import org.apache.gearpump.partitioner.{PartitionerByClassName, PartitionerDescription}
import org.apache.gearpump.services.MasterService.{BuiltinPartitioners, SubmitApplicationRequest}
// NOTE: This cannot be removed!!!
import org.apache.gearpump.services.util.UpickleUtil._
import org.apache.gearpump.streaming.{ProcessorDescription, ProcessorId, StreamApplication}
import org.apache.gearpump.util.ActorUtil._
import FileDirective._
import org.apache.gearpump.util.{Constants, Graph, Util}

/** Manages service for master node */
class MasterService(val master: ActorRef,
    val jarStoreClient: JarStoreClient, override val system: ActorSystem)
  extends BasicService {

  import upickle.default.{read, write}

  private val systemConfig = system.settings.config
  private val concise = systemConfig.getBoolean(Constants.GEARPUMP_SERVICE_RENDER_CONFIG_CONCISE)

  protected override def doRoute(implicit mat: Materializer) = pathPrefix("master") {
    pathEnd {
      get {
        onComplete(askActor[MasterData](master, GetMasterData)) {
          case Success(value: MasterData) => complete(write(value))
          case Failure(ex) => failWith(ex)
        }
      }
    } ~
    path("applist") {
      onComplete(askActor[AppMastersData](master, AppMastersDataRequest)) {
        case Success(value: AppMastersData) =>
          complete(write(value))
        case Failure(ex) => failWith(ex)
      }
    } ~
    path("workerlist") {
      def future: Future[List[WorkerSummary]] = askActor[WorkerList](master, GetAllWorkers)
        .flatMap { workerList =>
          val workers = workerList.workers
          val workerDataList = List.empty[WorkerSummary]

          Future.fold(workers.map { workerId =>
            askWorker[WorkerData](master, workerId, GetWorkerData(workerId))
          })(workerDataList) { (workerDataList, workerData) =>
            workerDataList :+ workerData.workerDescription
          }
        }
      onComplete(future) {
        case Success(result: List[WorkerSummary]) => complete(write(result))
        case Failure(ex) => failWith(ex)
      }
    } ~
    path("config") {
      onComplete(askActor[MasterConfig](master, QueryMasterConfig)) {
        case Success(value: MasterConfig) =>
          val config = Option(value.config).map(ClusterConfig.render(_, concise)).getOrElse("{}")
          complete(config)
        case Failure(ex) =>
          failWith(ex)
      }
    } ~
    path("metrics" / RestPath) { path =>
      parameters(ParamMagnet(ReadOption.Key ? ReadOption.ReadLatest)) { readOption: String =>
        val query = QueryHistoryMetrics(path.head.toString, readOption)
        onComplete(askActor[HistoryMetrics](master, query)) {
          case Success(value) =>
            complete(write(value))
          case Failure(ex) =>
            failWith(ex)
        }
      }
    } ~
    path("submitapp") {
      post {
        uploadFile { form =>
          val jar = form.getFileInfo("jar").map(_.file)
          val configFile = form.getFileInfo("configfile").map(_.file)
          val configString = form.getValue("configstring").getOrElse("")
          val executorCount = form.getValue("executorcount").getOrElse("1").toInt
          val args = form.getValue("args").getOrElse("")

          val mergedConfigFile = mergeConfig(configFile, configString)

          onComplete(Future(
            MasterService.submitGearApp(jar, executorCount, args, systemConfig, mergedConfigFile)
          )) {
            case Success(success) =>
              val response = MasterService.AppSubmissionResult(success)
              complete(write(response))
            case Failure(ex) =>
              failWith(ex)
          }
        }
      }
    } ~
    path("submitstormapp") {
      post {
        uploadFile { form =>
          val jar = form.getFileInfo("jar").map(_.file)
          val configFile = form.getFileInfo("configfile").map(_.file)
          val args = form.getValue("args").getOrElse("")
          onComplete(Future(
            MasterService.submitStormApp(jar, configFile, args, systemConfig)
          )) {
            case Success(success) =>
              val response = MasterService.AppSubmissionResult(success)
              complete(write(response))
            case Failure(ex) =>
              failWith(ex)
          }
        }
      }
    } ~
    path("submitdag") {
      post {
        entity(as[String]) { request =>
          val msg = java.net.URLDecoder.decode(request, "UTF-8")
          val submitApplicationRequest = read[SubmitApplicationRequest](msg)
          import submitApplicationRequest.{appName, dag, processors, userconfig}
          val context = ClientContext(system.settings.config, system, master)

          val graph = dag.mapVertex { processorId =>
            processors(processorId)
          }.mapEdge { (node1, edge, node2) =>
            PartitionerDescription(new PartitionerByClassName(edge))
          }

          val effectiveConfig = if (userconfig == null) UserConfig.empty else userconfig
          val appId = context.submit(new StreamApplication(appName, effectiveConfig, graph))

          import upickle.default.write
          val submitApplicationResultValue = SubmitApplicationResultValue(appId)
          val jsonData = write(submitApplicationResultValue)
          complete(jsonData)
        }
      }
    } ~
    path("uploadjar") {
      uploadFile { form =>
        val jar = form.getFileInfo("jar").map(_.file)
        if (jar.isEmpty) {
          complete(write(
            MasterService.Status(success = false, reason = "Jar file not found")))
        } else {
          val jarFile = Util.uploadJar(jar.get, jarStoreClient)
          complete(write(jarFile))
        }
      }
    } ~
    path("partitioners") {
      get {
        complete(write(BuiltinPartitioners(Constants.BUILTIN_PARTITIONERS.map(_.getName))))
      }
    }
  }

  private def mergeConfig(configFile: Option[File], configString: String): Option[File] = {
    if (configString == null || configString.isEmpty) {
      configFile
    } else {
      configFile match {
        case Some(file) =>
          Files.write(file.toPath, ("\n" + configString).getBytes(UTF_8), APPEND)
          Some(file)
        case None =>
          val file = File.createTempFile("\"userfile_configstring_", ".conf")
          Files.write(file.toPath, configString.getBytes(UTF_8), WRITE)
          Some(file)
      }
    }
  }
}

object MasterService {

  case class BuiltinPartitioners(partitioners: Array[String])

  case class AppSubmissionResult(success: Boolean)

  case class Status(success: Boolean, reason: String = null)

  /**
   * Submits Native Application.
   */
  def submitGearApp(
      jar: Option[File], executorNum: Int, args: String,
      systemConfig: Config, userConfigFile: Option[File]): Boolean = {
    submitAndDeleteTempFiles(
      "org.apache.gearpump.cluster.main.AppSubmitter",
      argsArray = Array("-executors", executorNum.toString) ++ spaceSeparatedArgumentsToArray(args),
      fileMap = Map("jar" -> jar).filter(_._2.isDefined).mapValues(_.get),
      classPath = getUserApplicationClassPath,
      systemConfig,
      userConfigFile
    )
  }

  /**
   * Submits Storm application.
   */
  def submitStormApp(
      jar: Option[File], stormConf: Option[File], args: String, systemConfig: Config): Boolean = {
    submitAndDeleteTempFiles(
      "org.apache.gearpump.experiments.storm.main.GearpumpStormClient",
      argsArray = spaceSeparatedArgumentsToArray(args),
      fileMap = Map("jar" -> jar, "config" -> stormConf).filter(_._2.isDefined).mapValues(_.get),
      classPath = getStormApplicationClassPath,
      systemConfig,
      userConfigFile = None
    )
  }

  private def submitAndDeleteTempFiles(
      mainClass: String, argsArray: Array[String], fileMap: Map[String, File],
      classPath: Array[String], systemConfig: Config,
      userConfigFile: Option[File] = None): Boolean = {
    try {
      val jar = fileMap.get("jar")
      if (jar.isEmpty) {
        throw new IOException("JAR file not supplied")
      }

      val process = Util.startProcess(
        clusterOptions(systemConfig, userConfigFile),
        classPath,
        mainClass,
        arguments = createFilePathArgArray(fileMap) ++ argsArray
      )

      val retval = process.exitValue()
      if (retval != 0) {
        throw new IOException(s"Process exit abnormally with exit code $retval.\n" +
          s"Error message: ${process.logger.error}")
      }
      true
    } finally {
      fileMap.values.foreach(_.delete)
      if (userConfigFile.isDefined) {
        userConfigFile.get.delete()
      }
    }
  }

  /**
   * Returns Java options for gearpump cluster
   */
  private def clusterOptions(systemConfig: Config, userConfigFile: Option[File]): Array[String] = {
    var options = Array(
      s"-D${Constants.GEARPUMP_HOME}=${systemConfig.getString(Constants.GEARPUMP_HOME)}",
      s"-D${Constants.GEARPUMP_HOSTNAME}=${systemConfig.getString(Constants.GEARPUMP_HOSTNAME)}",
      s"-D${Constants.PREFER_IPV4}=true"
    )

    val masters = systemConfig.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).asScala
      .toList.flatMap(Util.parseHostList)
    options ++= masters.zipWithIndex.map { case (master, index) =>
      s"-D${Constants.GEARPUMP_CLUSTER_MASTERS}.$index=${master.host}:${master.port}"
    }.toArray[String]

    if (userConfigFile.isDefined) {
      options :+= s"-D${Constants.GEARPUMP_CUSTOM_CONFIG_FILE}=${userConfigFile.get.getPath}"
    }
    options
  }

  /**
   * Filter all defined file paths and store their config key and path into an array.
   */
  private def createFilePathArgArray(fileMap: Map[String, File]): Array[String] = {
    var args = Array.empty[String]
    fileMap.foreach({ case (key, path) =>
      args ++= Array(s"-$key", path.getPath)
    })
    args
  }

  /**
   * Returns a space separated arguments as an array.
   */
  private def spaceSeparatedArgumentsToArray(str: String): Array[String] = {
    str.split(" +").filter(_.nonEmpty)
  }

  private val homeDir = System.getProperty(Constants.GEARPUMP_HOME) + "/"
  private val libHomeDir = homeDir + "lib/"

  private def getUserApplicationClassPath: Array[String] = {
    Array(
      homeDir + "conf",
      libHomeDir + "daemon/*",
      libHomeDir + "yarn/*",
      libHomeDir + "*"
    )
  }

  private def getStormApplicationClassPath: Array[String] = {
    getUserApplicationClassPath ++ Array(
      libHomeDir + "storm/*"
    )
  }

  case class SubmitApplicationRequest(
      appName: String,
      processors: Map[ProcessorId, ProcessorDescription],
      dag: Graph[Int, String],
      userconfig: UserConfig)
}
