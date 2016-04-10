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

package io.gearpump.experiments.yarn.glue

import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.client.api

import io.gearpump.experiments.yarn.glue.Records._
import io.gearpump.util.LogUtil

/**
 * Adapter for api.YarnClient
 */
class YarnClient(yarn: YarnConfig) {

  val LOG = LogUtil.getLogger(getClass)

  private val client: api.YarnClient = api.YarnClient.createYarnClient
  client.init(yarn.conf)
  client.start()
  LOG.info("Starting YarnClient...")

  def createApplication: ApplicationId = {
    val app = client.createApplication()
    val response = app.getNewApplicationResponse()
    LOG.info("Create application, appId: " + response.getApplicationId())
    response.getApplicationId()
  }

  def getApplicationReport(appId: ApplicationId): ApplicationReport = {
    client.getApplicationReport(appId)
  }

  def submit(
      name: String, appId: ApplicationId, command: String, resource: Resource, queue: String,
      packagePath: String, configPath: String): ApplicationId = {

    val appContext = Records.newAppSubmissionContext
    appContext.setApplicationName(name)
    appContext.setApplicationId(appId)

    val containerContext = ContainerLaunchContext(yarn.conf, command, packagePath, configPath)
    appContext.setAMContainerSpec(containerContext)
    appContext.setResource(resource)
    appContext.setQueue(queue)

    LOG.info(s"Submit Application $appId to YARN...")
    client.submitApplication(appContext)
  }

  def awaitApplication(appId: ApplicationId, timeoutMilliseconds: Long = Long.MaxValue)
    : ApplicationReport = {
    import YarnApplicationState._
    val terminated = Set(FINISHED, KILLED, FAILED, RUNNING)
    var result: ApplicationReport = null
    var done = false


    val start = System.currentTimeMillis()
    def timeout: Boolean = {
      val now = System.currentTimeMillis()
      if (now - start > timeoutMilliseconds) {
        true
      } else {
        false
      }
    }

    while (!done && !timeout) {
      val report = client.getApplicationReport(appId)
      val status = report.getYarnApplicationState
      if (terminated.contains(status)) {
        done = true
        result = report
      } else {
        Console.print(".")
        Thread.sleep(1000)
      }
    }

    if (timeout) {
      throw new Exception(s"Launch Application $appId timeout...")
    }
    result
  }

  def stop(): Unit = {
    client.stop()
  }
}