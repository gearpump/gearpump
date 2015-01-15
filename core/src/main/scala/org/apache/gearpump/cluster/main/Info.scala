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
package org.apache.gearpump.cluster.main

import org.apache.gearpump.cluster.MasterToAppMaster.AppMastersData
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

object Info extends App with ArgumentsParser {

  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master"-> CLIOption("<host1:port1,host2:port2,host3:port3>", required = true))

  def start : Unit = {
    val config = parse(args)
    if (null == config) {
      return
    }

    val masters = config.getString("master")
    Console.out.println("Master URL: " + masters)

    val client = ClientContext(masters)

    val AppMastersData(appMasters) = client.listApps
    appMasters.foreach { appData =>
      Console.println("== Application Information ==")
      Console.println("====================================")
      Console.println(s"application: ${appData.appId}, worker: ${appData.workerPath}")
    }
    client.close()
  }

  start
}