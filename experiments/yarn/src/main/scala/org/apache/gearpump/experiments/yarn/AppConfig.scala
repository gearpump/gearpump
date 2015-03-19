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

package org.apache.gearpump.experiments.yarn

import org.apache.gearpump.cluster.main.{ParseResult, ArgumentsParser, CLIOption}
import com.typesafe.config.Config

object EnvVars {  
  val YARNAPPMASTER_NAME = "gearpump.yarn.applicationmaster.name"
  val YARNAPPMASTER_COMMAND = "gearpump.yarn.applicationmaster.command"
  val YARNAPPMASTER_MEMORY = "gearpump.yarn.applicationmaster.memory"
  val YARNAPPMASTER_VCORES = "gearpump.yarn.applicationmaster.vcores"
  val YARNAPPMASTER_QUEUE = "gearpump.yarn.applicationmaster.queue"
  val YARNAPPMASTER_MAIN = "gearpump.yarn.applicationmaster.main"
  val YARNAPPMASTER_PORT = "gearpump.yarn.applicationmaster.port"
  val CONTAINER_COUNT = "gearpump.yarn.container.count"
  val CONTAINER_MEMORY = "gearpump.yarn.container.memory"
  val CONTAINER_VCORES = "gearpump.yarn.container.vcores"
  val EXCLUDE_JARS = "gearpump.yarn.client.excludejars"
  val HDFS_PATH = "gearpump.yarn.client.hdfsPath"
  val JARS = "gearpump.yarn.client.jars"
  val GEARPUMPMASTER_COMMAND = "gearpump.master.command"
  val GEARPUMPMASTER_MAIN = "gearpump.master.main"
  val GEARPUMPMASTER_IP = "gearpump.master.ip"
  val GEARPUMPMASTER_PORT = "gearpump.master.port"
  val WORKER_COMMAND = "gearpump.worker.command"
  val WORKER_MAIN = "gearpump.worker.main"
  val SERVICES_COMMAND = "gearpump.services.command"
  val SERVICES_MAIN = "gearpump.services.main"
}

object CmdLineVars {
  val APPMASTER_IP = "ip"
  val APPMASTER_PORT = "port"
}

class AppConfig(cliopts: ParseResult, conf: Config) {
  
   def getEnv(key: String): String = {
    Option(cliopts) match {
      case Some(_cliopts) =>
        _cliopts.exists(key) match {
          case true =>
            _cliopts.getString(key)
          case false =>
            conf.getString(key)
        }
      case None =>
        conf.getString(key)
    }
  }
}

