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

package io.gearpump.experiments.yarn.client

import java.io.IOException

import akka.actor.{ActorRef, ActorSystem}
import io.gearpump.experiments.yarn.glue.Records.ApplicationId
import io.gearpump.experiments.yarn.glue.YarnClient
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod

import scala.util.Try

/**
 * Resolve AppMaster ActorRef
 */
class AppMasterResolver(yarnClient: YarnClient, system: ActorSystem) {

  def resolve(appId: ApplicationId, timeoutSeconds: Int = 30): ActorRef = {
    retry(connect(appId), timeoutSeconds)
  }

  private def connect(appId: ApplicationId): ActorRef = {
    val report = yarnClient.getApplicationReport(appId)
    val client = new HttpClient()
    val appMasterPath = s"${report.getOriginalTrackingUrl}/supervisor-actor-path"
    val get = new GetMethod(appMasterPath)
    var status = client.executeMethod(get)
    if (status == 200) {
      system.actorFor(get.getResponseBodyAsString)
    } else {
      throw new IOException("Fail to resolve AppMaster address, please make sure " +
        s"${report.getOriginalTrackingUrl} is accessible...")
    }
  }

  private def retry(fun: => ActorRef, times: Int): ActorRef = {
    var index = 0
    var result: ActorRef = null
    while (index < 30 && result == null) {
      index += 1
      val tryConnect = Try(fun)
      if (tryConnect.isFailure) {
        Console.err.println(s"Failed to connect(tried $index)... "  + tryConnect.failed.get.getMessage)
        Thread.sleep(1000)
      } else {
        result = tryConnect.get
      }
    }
    result
  }
}
