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

package org.apache.gearpump.util

object Constants {

  //config for construction of appMaster
  val APPID = "appId"
  val APP_DESCRIPTION =  "appDescription"
  val SINGLETON_MANAGER = "singleton"
  val MASTER = "master"
  val MASTER_WATCHER = "masterwatcher"

  val WORKER = "worker"
  val WORKER_INFO = "workerinfo"
  val WORKER_RESOURCE = "worker.akka.cluster.resource"

  val APP_MASTER_REGISTER_DATA = "appmasterregisterdata"

  val GEARPUMP_SCHEDULING_SCHEDULER = "gearpump.scheduling.scheduler-class"
  val GEARPUMP_SCHEDULING_REQUEST = "gearpump.scheduling.requests"

  val GEARPUMP_SERIALIZERS = "gearpump.serializers"

  val GEARPUMP_TASK_DISPATCHER = "gearpump.task-dispatcher"
  val GEARPUMP_CLUSTER_MASTERS = "gearpump.cluster.masters"
  val GEARPUMP_APPMASTER_ARGS = "gearpump.streaming.appmaster.vmargs"
  val GEARPUMP_EXECUTOR_ARGS = "gearpump.streaming.executor.vmargs"

  //config for construction of executor
  val APP_MASTER = "appMaster"
  val EXECUTOR_ID = "executorId"
  val RESOURCE = "resource"

  val TASK_ID = "taskId"
  val TASK_DAG = "taskDag"

  val HADOOP_CONF = "hadoopConf"
}
