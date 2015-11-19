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

import io.gearpump.cluster.MasterToAppMaster
import io.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMastersData}
import io.gearpump.integrationtest.Docker
import upickle.default._

/**
 * A REST client to operate a Gearpump cluster
 */
class RestClient(host: String, port: Int) {

  def queryVersion(): String = {
    callFromRoot("version")
  }

  def listApps(): List[AppMasterData] = try {
    val resp = callApi("master/applist")
    read[AppMastersData](resp).appMasters
  } catch {
    case ex: Throwable => List.empty
  }

  def listRunningApps(): List[AppMasterData] = {
    listApps().filter(_.status == MasterToAppMaster.AppMasterActive)
  }

  def submitApp(jar: String, args: String = "", config: String = ""): Boolean = try {
    var endpoint = "master/submitapp"
    if (args.length > 0) {
      endpoint += "?args=" + Util.encodeUriComponent(args)
    }
    var options = Seq(s"jar=@$jar")
    if (config.length > 0) {
      options :+= s"conf=@$config"
    }
    val resp = callApi(endpoint, options.map("-F " + _).mkString(" "))
    resp.contains("\"success\":true")
  } catch {
    case ex: Throwable => false
  }

  def queryApp(appId: Int): AppMasterData = try {
    val resp = callApi(s"appmaster/$appId")
    read[AppMasterData](resp)
  } catch {
    case ex: Throwable => null
  }

  /*
  def queryAppDetail(appId: Int): AppMasterSummary = {
      val resp = callApi(s"appmaster/$appId?detail=true")
      if (resp.startsWith("java.lang.Exception: Can not find Application:"))
        null
      else upickle.default.read[AppMasterSummary](resp)
    }
    */

  def killApp(appId: Int): Boolean = try {
    val resp = callApi(s"appmaster/$appId", "-X DELETE")
    resp.contains("\"status\":\"success\"")
  } catch {
    case ex: Throwable => false
  }

  private def callApi(endpoint: String, options: String = ""): String = {
    callFromRoot(s"api/v1.0/$endpoint", options)
  }

  private def callFromRoot(endpoint: String, options: String = ""): String = {
    Docker.execAndCaptureOutput(host, s"curl -s $options http://$host:$port/$endpoint")
  }

}