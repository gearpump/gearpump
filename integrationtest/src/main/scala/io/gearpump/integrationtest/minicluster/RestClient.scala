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
package io.gearpump.integrationtest.minicluster

import com.typesafe.config.{Config, ConfigFactory}
import io.gearpump.cluster.MasterToAppMaster
import io.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMastersData}
import io.gearpump.cluster.MasterToClient.HistoryMetrics
import io.gearpump.integrationtest.{Docker, Util}
import io.gearpump.services.AppMasterService.Status
import io.gearpump.services.MasterService.{AppSubmissionResult, BuiltinPartitioners}
import io.gearpump.streaming.ProcessorDescription
import io.gearpump.cluster.AppMasterToMaster.MasterData
import io.gearpump.cluster.master.MasterSummary
import io.gearpump.cluster.worker.WorkerSummary
import io.gearpump.streaming.appmaster.AppMaster.ExecutorBrief
import io.gearpump.streaming.appmaster.DagManager.{DAGOperationResult, ReplaceProcessor}
import io.gearpump.streaming.appmaster.StreamAppMasterSummary
import io.gearpump.streaming.executor.Executor.ExecutorSummary
import io.gearpump.util.{Constants, Graph}
import upickle.Js
import upickle.default._

/**
 * A REST client to operate a Gearpump cluster
 */
class RestClient(host: String, port: Int) {

  implicit val graphReader: upickle.default.Reader[Graph[Int, String]] = upickle.default.Reader[Graph[Int, String]] {
    case Js.Obj(verties, edges) =>
      val vertexList = upickle.default.readJs[List[Int]](verties._2)
      val edgeList = upickle.default.readJs[List[(Int, String, Int)]](edges._2)
      Graph(vertexList, edgeList)
  }

  def queryVersion(): String = {
    callFromRoot("version")
  }

  def listWorkers(): Array[WorkerSummary] = {
    val resp = callApi("master/workerlist")
    read[List[WorkerSummary]](resp).toArray
  }

  def listRunningWorkers(): Array[WorkerSummary] = {
    listWorkers().filter(_.state == MasterToAppMaster.AppMasterActive)
  }

  def listApps(): Array[AppMasterData] = {
    val resp = callApi("master/applist")
    read[AppMastersData](resp).appMasters.toArray
  }

  def listRunningApps(): Array[AppMasterData] = {
    listApps().filter(_.status == MasterToAppMaster.AppMasterActive)
  }

  def submitApp(jar: String, args: String = "", config: String = ""): Int = try {
    var endpoint = "master/submitapp"
    if (args.length > 0) {
      endpoint += "?args=" + Util.encodeUriComponent(args)
    }
    var options = Seq(s"jar=@$jar")
    if (config.length > 0) {
      options :+= s"conf=@$config"
    }
    val resp = callApi(endpoint, options.map("-F " + _).mkString(" "))
    val result = read[AppSubmissionResult](resp)
    assert(result.success)
    result.appId
  } catch {
    case ex: Throwable => -1
  }

  def queryApp(appId: Int): AppMasterData = {
    val resp = callApi(s"appmaster/$appId")
    read[AppMasterData](resp)
  }

  def queryAppMasterConfig(appId: Int): Config = {
    val resp = callApi(s"appmaster/$appId/config")
    ConfigFactory.parseString(resp)
  }

  def queryStreamingAppDetail(appId: Int): StreamAppMasterSummary = {
    val resp = callApi(s"appmaster/$appId?detail=true")
    upickle.default.read[StreamAppMasterSummary](resp)
  }

  def queryStreamingAppMetrics(appId: Int, current: Boolean): HistoryMetrics = {
    val args = if (current) "?readLatest=true" else ""
    val resp = callApi(s"appmaster/$appId/metrics/app$appId.processor*$args")
    upickle.default.read[HistoryMetrics](resp)
  }

  def queryExecutorSummary(appId: Int, executorId: Int): ExecutorSummary = {
    val resp = callApi(s"appmaster/$appId/executor/$executorId")
    upickle.default.read[ExecutorSummary](resp)
  }

  def queryExecutorBrief(appId: Int): Array[ExecutorBrief] = {
    queryStreamingAppDetail(appId).executors.toArray
  }

  def queryExecutorMetrics(appId: Int, current: Boolean): HistoryMetrics = {
    val args = if (current) "?readLatest=true" else ""
    val resp = callApi(s"appmaster/$appId/metrics/app$appId.executor*$args")
    upickle.default.read[HistoryMetrics](resp)
  }

  def queryExecutorConfig(appId: Int, executorId: Int): Config = {
    val resp = callApi(s"appmaster/$appId/executor/$executorId/config")
    ConfigFactory.parseString(resp)
  }

  def queryMaster(): MasterSummary = {
    val resp = callApi("master")
    read[MasterData](resp).masterDescription
  }

  def queryMasterMetrics(current: Boolean): HistoryMetrics = {
    val args = if (current) "?readLatest=true" else ""
    val resp = callApi(s"master/metrics/master?$args")
    upickle.default.read[HistoryMetrics](resp)
  }

  def queryMasterConfig(): Config = {
    val resp = callApi("master/config")
    ConfigFactory.parseString(resp)
  }

  def queryWorkerMetrics(workerId: Int, current: Boolean): HistoryMetrics = {
    val args = if (current) "?readLatest=true" else ""
    val resp = callApi(s"worker/$workerId/metrics/worker$workerId?$args")
    upickle.default.read[HistoryMetrics](resp)
  }

  def queryWorkerConfig(workerId: Int): Config = {
    val resp = callApi(s"worker/$workerId/config")
    ConfigFactory.parseString(resp)
  }

  def queryBuiltInPartitioners(): Array[String] = {
    val resp = callApi("master/partitioners")
    upickle.default.read[BuiltinPartitioners](resp).partitioners
  }

  def replaceStreamingAppProcessor(appId: Int, replaceMe: ProcessorDescription): Boolean = try {
    val replaceOperation = new ReplaceProcessor(replaceMe.id, replaceMe)
    val args = upickle.default.write(replaceOperation)
    val resp = callApi(s"appmaster/$appId/dynamicdag?args=" + Util.encodeUriComponent(args),
      CRUD_POST + " -F ignore=ignore")
    upickle.default.read[DAGOperationResult](resp)
    true
  } catch {
    case ex: Throwable => false
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
    case ex: Throwable => false
  }

  def killApp(appId: Int): Boolean = try {
    val resp = callApi(s"appmaster/$appId", CRUD_DELETE)
    resp.contains("\"status\":\"success\"")
  } catch {
    case ex: Throwable => false
  }

  def restartApp(appId: Int): Boolean = try {
    val resp = callApi(s"appmaster/$appId/restart", CRUD_POST)
    upickle.default.read[Status](resp)
    // return true here for the api always returns false
    // please go to issue #1600 for detail
    true
  } catch {
    case ex: Throwable => false
  }

  private val CRUD_POST = "-X POST"
  private val CRUD_DELETE = "-X DELETE"

  private def callApi(endpoint: String, options: String = ""): String = {
    callFromRoot(s"api/v1.0/$endpoint", options)
  }

  private def callFromRoot(endpoint: String, options: String = ""): String = {
    Docker.execAndCaptureOutput(host, s"curl -s $options http://$host:$port/$endpoint")
  }

}