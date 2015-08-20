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

package io.gearpump.experiments.yarn

object Constants {
  val YARNAPPMASTER_NAME = "gearpump.yarn.applicationmaster.name"
  val YARNAPPMASTER_COMMAND = "gearpump.yarn.applicationmaster.command"
  val YARNAPPMASTER_MEMORY = "gearpump.yarn.applicationmaster.memory"
  val YARNAPPMASTER_VCORES = "gearpump.yarn.applicationmaster.vcores"
  val YARNAPPMASTER_QUEUE = "gearpump.yarn.applicationmaster.queue"
  val YARNAPPMASTER_MAIN = "gearpump.yarn.applicationmaster.main"
  val YARNAPPMASTER_PORT = "gearpump.yarn.applicationmaster.port"

  val EXCLUDE_JARS = "gearpump.yarn.client.excludejars"
  val HDFS_ROOT = "gearpump.yarn.client.hdfsRoot"
  val JARS = "gearpump.yarn.client.jars"
  val GEARPUMPMASTER_COMMAND = "gearpump.master.command"
  val GEARPUMPMASTER_MAIN = "gearpump.master.main"
  val GEARPUMPMASTER_IP = "gearpump.master.ip"
  val GEARPUMPMASTER_PORT = "gearpump.master.port"
  val GEARPUMPMASTER_CONTAINERS = "gearpump.master.containers"
  val GEARPUMPMASTER_MEMORY = "gearpump.master.memory"
  val GEARPUMPMASTER_VCORES = "gearpump.master.vcores"
  val GEARPUMPMASTER_LOG = "gearpump.master.logname"

  val WORKER_COMMAND = "gearpump.worker.command"
  val WORKER_MAIN = "gearpump.worker.main"
  val WORKER_CONTAINERS = "gearpump.worker.containers"
  val WORKER_MEMORY = "gearpump.worker.memory"
  val WORKER_VCORES = "gearpump.worker.vcores"
  val WORKER_LOG = "gearpump.worker.logname"

  val SERVICES_COMMAND = "gearpump.services.command"
  val SERVICES_MAIN = "gearpump.services.main"
  val SERVICES_PORT = "gearpump.services.port"
  val SERVICES_CONTAINERS = "gearpump.services.containers"
  val SERVICES_MEMORY = "gearpump.services.memory"
  val SERVICES_VCORES = "gearpump.services.vcores"
  val SERVICES_LOG = "gearpump.services.logname"

  val YARN_CONFIG = "gearpump_on_yarn.conf"
  val MEMORY_DEFAULT = 2048 //2 gigabyte

}
