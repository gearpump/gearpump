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
import io.gearpump.integrationtest.Docker

/**
 * A command-line client to operate a Gearpump cluster
 */
class CommandLineClient(host: String) {

  def listApps(): Array[String] = {
    execAndCaptureOutput("gear info").split("\n").filter(
      _.startsWith("application: ")
    )
  }

  def listRunningApps(): Array[String] =
    listApps().filter(
      _.contains(s", status: ${MasterToAppMaster.AppMasterActive}")
    )

  def queryApp(appId: Int): String = try {
    listApps().filter(
      _.startsWith(s"application: $appId")
    ).head
  } catch {
    case ex: Throwable => ""
  }

  def submitAppAndCaptureOutput(jar: String, args: String = ""): String = {
    execAndCaptureOutput(s"gear app -verbose true -jar $jar $args")
  }

  def submitApp(jar: String, args: String = ""): Int = {
    submitAppUse("gear app", jar, args)
  }

  private def submitAppUse(launcher: String, jar: String, args: String = ""): Int = try {
    execAndCaptureOutput(s"$launcher -jar $jar $args").split("\n").last
      .replace("Submit application succeed. The application id is ", "")
      .toInt
  } catch {
    case ex: Throwable => -1
  }

  def killApp(appId: Int): Boolean = {
    exec(s"gear kill -appid $appId")
  }

  private def exec(command: String): Boolean = {
    Docker.exec(host, s"/opt/start $command")
  }

  private def execAndCaptureOutput(command: String): String = {
    Docker.execAndCaptureOutput(host, s"/opt/start $command")
  }

}
