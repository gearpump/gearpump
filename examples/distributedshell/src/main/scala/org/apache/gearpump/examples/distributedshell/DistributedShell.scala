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
package org.apache.gearpump.examples.distributedshell

import org.slf4j.Logger

import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.cluster.{Application, UserConfig}
import org.apache.gearpump.util.{AkkaApp, LogUtil}

/** Demo application to distribute and execute shell command on all machines of the cluster */
object DistributedShell extends AkkaApp with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array.empty

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    LOG.info(s"Distributed shell submitting application...")
    val context = ClientContext(akkaConf)
    val app = context.submit(Application[DistShellAppMaster]("DistributedShell",
    UserConfig.empty))
    context.close()
    LOG.info(s"Distributed Shell Application started with appId ${app.appId} !")
  }
}
