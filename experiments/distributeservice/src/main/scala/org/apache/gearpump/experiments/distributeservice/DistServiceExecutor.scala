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
package org.apache.gearpump.experiments.distributeservice

import java.io.File

import akka.actor.Actor
import org.apache.commons.io.FileUtils
import org.apache.gearpump.cluster.{UserConfig, ExecutorContext}
import org.apache.gearpump.experiments.distributeservice.DistServiceAppMaster.DistributeFile
import org.apache.gearpump.util.{FileServer, LogUtil}
import org.slf4j.Logger

class DistServiceExecutor(executorContext: ExecutorContext, userConf : UserConfig) extends Actor {
  import executorContext._
  private val LOG: Logger = LogUtil.getLogger(getClass, executor = executorId, app = appId)

  LOG.info(s"ShellExecutor started!")
  override def receive: Receive = {
    case DistributeFile(url, fileName) =>
      val tempFile = File.createTempFile(s"executor$executorId", fileName)
      val client = FileServer.newClient
      val bytes = client.get(url).get
      FileUtils.writeByteArrayToFile(tempFile, bytes)
      LOG.info(s"executor $executorId retrieve file $fileName to local ${tempFile.getPath}")
  }
}
