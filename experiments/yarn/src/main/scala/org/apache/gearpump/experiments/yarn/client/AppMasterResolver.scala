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

package org.apache.gearpump.experiments.yarn.client

import java.io.IOException

import akka.actor.{ActorRef, ActorSystem}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.gearpump.experiments.yarn.glue.Records.ApplicationId
import org.apache.gearpump.experiments.yarn.glue.YarnClient
import org.apache.gearpump.util.{AkkaHelper, LogUtil}

import scala.util.Try

/**
 * Resolves AppMaster ActorRef
 */
class AppMasterResolver(yarnClient: YarnClient, system: ActorSystem) {
  val LOG = LogUtil.getLogger(getClass)
  val RETRY_INTERVAL_MS = 3000 // ms

  def resolve(appId: ApplicationId, timeoutSeconds: Int = 30): ActorRef = {
    val appMaster = retry(connect(appId), 1 + timeoutSeconds * 1000 / RETRY_INTERVAL_MS)
    appMaster
  }

  private def connect(appId: ApplicationId): ActorRef = {
    val report = yarnClient.getApplicationReport(appId)
    val client = new HttpClient()
    val appMasterPath = s"${report.getTrackingURL}/supervisor-actor-path"
    LOG.info(s"appMasterPath=$appMasterPath")
    val get = new GetMethod(appMasterPath)
    val status = client.executeMethod(get)
    if (status == 200) {
      val response = get.getResponseBodyAsString
      LOG.info("Successfully resolved AppMaster address: " + response)
      AkkaHelper.actorFor(system, response)
    } else {
      throw new IOException("Fail to resolve AppMaster address, please make sure " +
        s"${report.getTrackingURL} is accessible...")
    }
  }

  private def retry(fun: => ActorRef, times: Int): ActorRef = {
    var index = 0
    var result: ActorRef = null
    while (index < times && result == null) {
      Thread.sleep(RETRY_INTERVAL_MS)
      index += 1
      val tryConnect = Try(fun)
      if (tryConnect.isFailure) {
        LOG.error(s"Failed to connect YarnAppMaster(tried $index)... " +
          tryConnect.failed.get.getMessage)
      } else {
        result = tryConnect.get
      }
    }
    result
  }
}
