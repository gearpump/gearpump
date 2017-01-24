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
package org.apache.gearpump.integrationtest.minicluster

import scala.reflect.ClassTag

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import upickle.Js
import upickle.default._

import org.apache.gearpump.cluster.ApplicationStatus._
import org.apache.gearpump.cluster.AppMasterToMaster.MasterData
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMastersData}
import org.apache.gearpump.cluster.MasterToClient.HistoryMetrics
import org.apache.gearpump.cluster.master.MasterSummary
import org.apache.gearpump.cluster.worker.{WorkerId, WorkerSummary}
import org.apache.gearpump.cluster.AppJar
import org.apache.gearpump.integrationtest.{Docker, Util}
import org.apache.gearpump.services.AppMasterService.Status
import org.apache.gearpump.services.MasterService.{AppSubmissionResult, BuiltinPartitioners}
// NOTE: This cannot be removed!!!
import org.apache.gearpump.services.util.UpickleUtil._
import org.apache.gearpump.streaming.ProcessorDescription
import org.apache.gearpump.streaming.appmaster.AppMaster.ExecutorBrief
import org.apache.gearpump.streaming.appmaster.DagManager.{DAGOperationResult, ReplaceProcessor}
import org.apache.gearpump.streaming.appmaster.StreamAppMasterSummary
import org.apache.gearpump.streaming.executor.Executor.ExecutorSummary
import org.apache.gearpump.util.{Constants, Graph}

/**
 * A REST client to operate a Gearpump cluster
 */
class RestClient(host: String, port: Int) {

  private val LOG = Logger.getLogger(getClass)

  private val cookieFile: String = "cookie.txt"

  implicit val graphReader: upickle.default.Reader[Graph[Int, String]] =
    upickle.default.Reader[Graph[Int, String]] {
    case Js.Obj(verties, edges) =>
      val vertexList = upickle.default.readJs[List[Int]](verties._2)
      val edgeList = upickle.default.readJs[List[(Int, String, Int)]](edges._2)
      Graph(vertexList, edgeList)
  }

  private def decodeAs[T](
      expr: String)(implicit reader: upickle.default.Reader[T], classTag: ClassTag[T]): T = try {
    read[T](expr)
  } catch {
    case ex: Throwable =>
      LOG.error(s"Failed to decode Rest response to ${classTag.runtimeClass.getSimpleName}")
      throw ex
  }

  def queryVersion(): String = {
    curl("version")
  }

  def listWorkers(): Array[WorkerSummary] = {
    val resp = callApi("master/workerlist")
    decodeAs[List[WorkerSummary]](resp).toArray
  }

  def listRunningWorkers(): Array[WorkerSummary] = {
    listWorkers().filter(_.state == ACTIVE.status)
  }

  def listApps(): Array[AppMasterData] = {
    val resp = callApi("master/applist")
    decodeAs[AppMastersData](resp).appMasters.toArray
  }

  def listPendingOrRunningApps(): Array[AppMasterData] = {
    listApps().filter(app => app.status == ACTIVE
      || app.status == PENDING)
  }

  def listRunningApps(): Array[AppMasterData] = {
    listApps().filter(_.status == ACTIVE)
  }

  def getNextAvailableAppId(): Int = {
    listApps().length + 1
  }

  def submitApp(jar: String, executorNum: Int, args: String = "", config: String = "")
    : Boolean = try {
    val endpoint = "master/submitapp"

    var options = Seq(s"jar=@$jar")
    if (config.length > 0) {
      options :+= s"conf=@$config"
    }

    options :+= s"executorcount=$executorNum"

    if (args != null && !args.isEmpty) {
      options :+= "args=\"" + args + "\""
    }

    val resp = callApi(endpoint, options.map("-F " + _).mkString(" "))
    val result = decodeAs[AppSubmissionResult](resp)
    assert(result.success)
    true
  } catch {
    case ex: Throwable =>
      LOG.warn(s"swallowed an exception: $ex")
      false
  }

  def queryApp(appId: Int): AppMasterData = {
    val resp = callApi(s"appmaster/$appId")
    decodeAs[AppMasterData](resp)
  }

  def queryAppMasterConfig(appId: Int): Config = {
    val resp = callApi(s"appmaster/$appId/config")
    ConfigFactory.parseString(resp)
  }

  def queryStreamingAppDetail(appId: Int): StreamAppMasterSummary = {
    val resp = callApi(s"appmaster/$appId?detail=true")
    decodeAs[StreamAppMasterSummary](resp)
  }

  def queryStreamingAppMetrics(appId: Int, current: Boolean, path: String = "processor*")
    : HistoryMetrics = {
    val args = if (current) "?readLatest=true" else ""
    val resp = callApi(s"appmaster/$appId/metrics/app$appId.$path$args")
    decodeAs[HistoryMetrics](resp)
  }

  def queryExecutorSummary(appId: Int, executorId: Int): ExecutorSummary = {
    val resp = callApi(s"appmaster/$appId/executor/$executorId")
    decodeAs[ExecutorSummary](resp)
  }

  def queryExecutorBrief(appId: Int): Array[ExecutorBrief] = {
    queryStreamingAppDetail(appId).executors.toArray
  }

  def queryExecutorMetrics(appId: Int, current: Boolean): HistoryMetrics = {
    val args = if (current) "?readLatest=true" else ""
    val resp = callApi(s"appmaster/$appId/metrics/app$appId.executor*$args")
    decodeAs[HistoryMetrics](resp)
  }

  def queryExecutorConfig(appId: Int, executorId: Int): Config = {
    val resp = callApi(s"appmaster/$appId/executor/$executorId/config")
    ConfigFactory.parseString(resp)
  }

  def queryMaster(): MasterSummary = {
    val resp = callApi("master")
    decodeAs[MasterData](resp).masterDescription
  }

  def queryMasterMetrics(current: Boolean): HistoryMetrics = {
    val args = if (current) "?readLatest=true" else ""
    val resp = callApi(s"master/metrics/master?$args")
    decodeAs[HistoryMetrics](resp)
  }

  def queryMasterConfig(): Config = {
    val resp = callApi("master/config")
    ConfigFactory.parseString(resp)
  }

  def queryWorkerMetrics(workerId: WorkerId, current: Boolean): HistoryMetrics = {
    val args = if (current) "?readLatest=true" else ""
    val workerIdStr = WorkerId.render(workerId)
    val resp = callApi(s"worker/$workerIdStr/metrics/worker$workerIdStr?$args")
    decodeAs[HistoryMetrics](resp)
  }

  def queryWorkerConfig(workerId: WorkerId): Config = {
    val resp = callApi(s"worker/${WorkerId.render(workerId)}/config")
    ConfigFactory.parseString(resp)
  }

  def queryBuiltInPartitioners(): Array[String] = {
    val resp = callApi("master/partitioners")
    decodeAs[BuiltinPartitioners](resp).partitioners
  }

  def uploadJar(localFilePath: String): AppJar = {
    val resp = callApi(s"master/uploadjar -F jar=@$localFilePath", CRUD_POST)
    decodeAs[AppJar](resp)
  }

  def replaceStreamingAppProcessor(appId: Int, replaceMe: ProcessorDescription): Boolean = {
    replaceStreamingAppProcessor(appId, replaceMe, false)
  }

  def replaceStreamingAppProcessor(
      appId: Int, replaceMe: ProcessorDescription, inheritConf: Boolean): Boolean = try {
    val replaceOperation = new ReplaceProcessor(replaceMe.id, replaceMe, inheritConf)
    val args = upickle.default.write(replaceOperation)
    val resp = callApi(s"appmaster/$appId/dynamicdag?args=" + Util.encodeUriComponent(args),
      CRUD_POST)
    decodeAs[DAGOperationResult](resp)
    true
  } catch {
    case ex: Throwable =>
      LOG.warn(s"swallowed an exception: $ex")
      false
  }

  def killAppMaster(appId: Int): Boolean = {
    killExecutor(appId, Constants.APPMASTER_DEFAULT_EXECUTOR_ID)
  }

  def killExecutor(appId: Int, executorId: Int): Boolean = try {
    val jvmInfo = queryExecutorSummary(appId, executorId).jvmName.split("@")
    val pid = jvmInfo(0).toInt
    val hostname = jvmInfo(1)
    Docker.killProcess(hostname, pid)
  } catch {
    case ex: Throwable =>
      LOG.warn(s"swallowed an exception: $ex")
      false
  }

  def killApp(appId: Int): Boolean = try {
    val resp = callApi(s"appmaster/$appId", CRUD_DELETE)
    resp.contains("\"status\":\"success\"")
  } catch {
    case ex: Throwable =>
      LOG.warn(s"swallowed an exception: $ex")
      false
  }

  def restartApp(appId: Int): Boolean = try {
    val resp = callApi(s"appmaster/$appId/restart", CRUD_POST)
    decodeAs[Status](resp).success
  } catch {
    case ex: Throwable =>
      LOG.warn(s"swallowed an exception: $ex")
      false
  }

  private val CRUD_POST = "-X POST"
  private val CRUD_DELETE = "-X DELETE"

  private def callApi(endpoint: String, option: String = ""): String = {
    curl(s"api/v1.0/$endpoint", Array(option, s"--cookie $cookieFile"))
  }

  private def curl(endpoint: String, options: Array[String] = Array.empty[String]): String = {
    Docker.curl(host, s"http://$host:$port/$endpoint", options)
  }

  def login(): Unit = {
    curl("login", Array(CRUD_POST, s"--cookie-jar $cookieFile",
      "--data username=admin", "--data password=admin"))
  }
}