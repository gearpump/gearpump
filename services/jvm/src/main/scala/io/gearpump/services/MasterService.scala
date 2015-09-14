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


package io.gearpump.services

import java.io.{File, IOException}

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.Unmarshaller._
import com.typesafe.config.Config
import io.gearpump.cluster.AppMasterToMaster.{GetAllWorkers, GetMasterData, GetWorkerData, MasterData, WorkerData}
import io.gearpump.cluster.ClientToMaster.{QueryHistoryMetrics, QueryMasterConfig}
import io.gearpump.cluster.MasterToAppMaster.{AppMastersData, AppMastersDataRequest, WorkerList}
import io.gearpump.cluster.MasterToClient.{HistoryMetrics, MasterConfig, SubmitApplicationResultValue}
import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.AppSubmitter
import io.gearpump.cluster.worker.WorkerSummary
import io.gearpump.partitioner.{PartitionerByClassName, PartitionerDescription}
import io.gearpump.streaming.StreamApplication
import io.gearpump.streaming.appmaster.SubmitApplicationRequest
import io.gearpump.util.ActorUtil.{askActor, _}
import io.gearpump.util.FileDirective._
import io.gearpump.util.{Constants, Util}
import io.gearpump.util.ActorUtil._
import io.gearpump.util.{Constants, FileUtils, Util}
import io.gearpump.services.MasterService.BuiltinPartitioners

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait MasterService {
  this: JarStoreProvider =>

  import upickle.default.{read, write}

  def master: ActorRef

  implicit def system: ActorSystem

  implicit def ec: ExecutionContext = system.dispatcher

  implicit val timeout = Constants.FUTURE_TIMEOUT

  def masterRoute: Route = {
    pathPrefix("api" / s"$REST_VERSION" / "master") {
      extractMaterializer { implicit mat =>
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
            def future = askActor[WorkerList](master, GetAllWorkers).flatMap { workerList =>
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
                val config = Option(value.config).map(_.root.render()).getOrElse("{}")
                complete(config)
              case Failure(ex) =>
                failWith(ex)
            }
          } ~
          path("metrics" / RestPath) { path =>
            parameter("readLatest" ? "false") { readLatestInput =>
              val readLatest = Try(readLatestInput.toBoolean).getOrElse(false)
              val query = QueryHistoryMetrics(path.head.toString, readLatest)
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
              parameters("args" ? "") { args: String =>
                uploadFile { fileMap =>
                  val jar = fileMap.get("jar").map(_.file)
                  val userConf = fileMap.get("conf").map(_.file)

                  if (jar.isEmpty) {
                    failWith(new Exception("jar file not supplied"))
                  } else {
                    val argsArray = args.split(" +")
                    onComplete(Future(
                      MasterService.submitJar(jar.get, userConf, argsArray, system.settings.config))) {
                      case Success(_) =>
                        complete(write(
                          MasterService.Status(success = true)))
                      case Failure(ex) =>
                        failWith(ex)
                    }
                  }
                }
              }
            }
          } ~
          path("submitdag") {
            post {
              entity(as[String]) { request =>
                import io.gearpump.services.util.UpickleUtil._
                val msg = java.net.URLDecoder.decode(request, "UTF-8")
                val submitApplicationRequest = read[SubmitApplicationRequest](msg)
                import submitApplicationRequest.{appName, dag, processors, userconfig}
                val context = ClientContext(system.settings.config, Some(system), Some(master))

                val graph = dag.mapVertex { processorId =>
                  processors(processorId)
                }.mapEdge { (node1, edge, node2) =>
                  PartitionerDescription(new PartitionerByClassName(edge))
                }

                if(graph.hasDuplicatedEdge()){
                  complete(write(MasterService.Status(success=false, reason="Graph has duplicated edges")))
                }

                val appId = context.submit(new StreamApplication(appName, userconfig, graph))

                import upickle.default.write
                val submitApplicationResultValue = SubmitApplicationResultValue(appId)
                val jsonData = write(submitApplicationResultValue)
                complete(jsonData)
              }
            }
          } ~
          path("uploadjar") {
            uploadFile { fileMap =>
              val jar = fileMap.get("jar").map(_.file)
              if (jar.isEmpty) {
                complete(write(
                  MasterService.Status(success = false, reason = "Jar file not found")))
              } else {
                val jarFile = Util.uploadJar(jar.get, getJarStoreService)
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
    }
  }
}

object MasterService {

  case class BuiltinPartitioners(partitioners: Array[String])

  case class Status(success: Boolean, reason: String = null)

  /**
   * Upload user application (JAR) into temporary directory and use AppSubmitter to submit
   * it to master. The temporary file will be removed after submission is done/failed.
   */
  def submitJar(jar: File, userConf: Option[File], extraArgs: Array[String],
                sysConfig: Config): Unit = {

    try {
      val masters = sysConfig.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).toList.flatMap(Util.parseHostList)
      val mastersOption = masters.zipWithIndex.map { kv =>
        val (master, index) = kv
        s"-D${Constants.GEARPUMP_CLUSTER_MASTERS}.${index}=${master.host}:${master.port}"
      }.toArray

      val hostname = sysConfig.getString(Constants.GEARPUMP_HOSTNAME)
      var options = Array(
        s"-D${Constants.GEARPUMP_HOSTNAME}=$hostname"
      ) ++ mastersOption

      if (userConf.isDefined) {
        options :+= s"-D${Constants.GEARPUMP_CUSTOM_CONFIG_FILE}=${userConf.get.getPath}"
      }

      val arguments = Array("-jar", jar.getPath) ++ extraArgs
      val mainClass = AppSubmitter.getClass.getName.dropRight(1)
      val process = Util.startProcess(options, Util.getCurrentClassPath, mainClass, arguments)
      val retval = process.exitValue()
      if (retval != 0) {
        throw new IOException(s"Process exit abnormally with code $retval, error summary: ${process.logger.summary}")
      }
    } finally {
      userConf.foreach(_.delete)
      jar.delete()
    }
  }
}
