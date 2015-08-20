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
package io.gearpump.experiments.distributeservice

import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.{Application, AppJar, UserConfig, AppDescription}
import io.gearpump.cluster.main.{ParseResult, CLIOption, ArgumentsParser}
import io.gearpump.util.{AkkaApp, LogUtil}
import org.slf4j.Logger

object DistributeService extends AkkaApp with ArgumentsParser  {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array.empty

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    LOG.info(s"Distribute Service submitting application...")
    val context = ClientContext(akkaConf)
    val appId = context.submit(Application[DistServiceAppMaster]("DistributedService", UserConfig.empty))
    context.close()
    LOG.info(s"Distribute Service Application started with appId $appId !")
  }
}
