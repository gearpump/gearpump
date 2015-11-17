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

import io.gearpump.integrationtest.Docker

/**
 * A command client to operate a Gearpump cluster
 */
class CommandClient(host: String) {

  def queryApps(): Array[String] = {
    val appsInfo = gearCommand("info").split("\n").filter(_.contains("application"))
    Thread.sleep(5000)
    appsInfo
  }

  def queryApp(appId: Int): String = {
    queryApps().filter(_.contains(s"application: $appId")).head
  }

  def submitApp(jar: String, args: String = ""): Boolean = {
    gearCommand("app", s"-jar $jar $args").contains("Submit application succeed")
  }

  def killApp(appId: Int): Unit = {
    gearCommand("kill", s"-appid $appId")
  }

  private def gearCommand(option: String, args: String = ""): String = {
    Docker.execAndCaptureOutput(host, s"/opt/start gear $option $args")
  }



}
