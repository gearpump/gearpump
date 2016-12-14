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

package org.apache.gearpump.experiments.yarn

object Constants {
  val CONTAINER_USER = "gearpump.yarn.user"
  val APPMASTER_NAME = "gearpump.yarn.applicationmaster.name"
  val APPMASTER_COMMAND = "gearpump.yarn.applicationmaster.command"
  val APPMASTER_MEMORY = "gearpump.yarn.applicationmaster.memory"
  val APPMASTER_VCORES = "gearpump.yarn.applicationmaster.vcores"
  val APPMASTER_QUEUE = "gearpump.yarn.applicationmaster.queue"

  val PACKAGE_PATH = "gearpump.yarn.client.package-path"
  val CONFIG_PATH = "gearpump.yarn.client.config-path"

  val MASTER_COMMAND = "gearpump.yarn.master.command"
  val MASTER_MEMORY = "gearpump.yarn.master.memory"
  val MASTER_VCORES = "gearpump.yarn.master.vcores"

  val WORKER_COMMAND = "gearpump.yarn.worker.command"
  val WORKER_CONTAINERS = "gearpump.yarn.worker.containers"
  val WORKER_MEMORY = "gearpump.yarn.worker.memory"
  val WORKER_VCORES = "gearpump.yarn.worker.vcores"

  val SERVICES_ENABLED = "gearpump.yarn.services.enabled"

  val LOCAL_DIRS = org.apache.hadoop.yarn.api.ApplicationConstants.Environment.LOCAL_DIRS.$$()
  val CONTAINER_ID = org.apache.hadoop.yarn.api.ApplicationConstants.Environment.CONTAINER_ID.$$()
  val LOG_DIR_EXPANSION_VAR = org.apache.hadoop.yarn.api.ApplicationConstants.LOG_DIR_EXPANSION_VAR
  val NODEMANAGER_HOST = org.apache.hadoop.yarn.api.ApplicationConstants.Environment.NM_HOST.$$()
}