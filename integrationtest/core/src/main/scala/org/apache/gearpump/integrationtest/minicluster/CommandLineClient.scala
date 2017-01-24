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

import org.apache.log4j.Logger
import org.apache.gearpump.cluster.ApplicationStatus
import org.apache.gearpump.integrationtest.Docker

/**
 * A command-line client to operate a Gearpump cluster
 */
class CommandLineClient(host: String) {

  private val LOG = Logger.getLogger(getClass)

  def listApps(): Array[String] = {
    gearCommand(host, "gear info").split("\n").filter(
      _.startsWith("application: ")
    )
  }

  def listRunningApps(): Array[String] =
    listApps().filter(_.contains(s", status: ${ApplicationStatus.ACTIVE}"))

  def queryApp(appId: Int): String = try {
    listApps().filter(_.startsWith(s"application: $appId")).head
  } catch {
    case ex: Throwable =>
      LOG.warn(s"swallowed an exception: $ex")
      ""
  }

  def submitAppAndCaptureOutput(jar: String, executorNum: Int, args: String = ""): String = {
    gearCommand(host, s"gear app -verbose true -jar $jar -executors $executorNum $args")
  }

  def submitApp(jar: String, args: String = ""): Int = {
    LOG.debug(s"|=> Submit Application $jar...")
    submitAppUse("gear app", jar, args)
  }

  private def submitAppUse(launcher: String, jar: String, args: String = ""): Int = try {
    gearCommand(host, s"$launcher -jar $jar $args").split("\n")
      .filter(_.contains("The application id is ")).head.split(" ").last.toInt
  } catch {
    case ex: Throwable =>
      LOG.warn(s"swallowed an exception: $ex")
      -1
  }

  def killApp(appId: Int): Boolean = {
    tryGearCommand(host, s"gear kill -appid $appId")
  }

  private def gearCommand(container: String, command: String): String = {
    LOG.debug(s"|=> Gear command $command in container $container...")
    Docker.execute(container, s"/opt/start $command")
  }

  private def tryGearCommand(container: String, command: String): Boolean = {
    LOG.debug(s"|=> Gear command $command in container $container...")
    Docker.executeSilently(container, s"/opt/start $command")
  }
}
