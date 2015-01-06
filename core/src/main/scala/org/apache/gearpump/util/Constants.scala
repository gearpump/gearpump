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

import java.util.concurrent.TimeUnit

object Constants {

  //config for construction of appMaster
  val APPID = "appId"
  val USERNAME = "username"
  val APP_DESCRIPTION =  "appDescription"
  val SINGLETON_MANAGER = "singleton"

  val SHARE = "share"
  val GEARPUMP_CONFIGS = "gearpump"
  val BASE = "base"
  val MASTER = "master"

  val MASTER_WATCHER = "masterwatcher"

  val WORKER = "worker"
  val WORKER_ID = "workerid"
  val WORKER_RESOURCE = "worker.akka.cluster.resource"

  val APP_MASTER_REGISTER_DATA = "appmasterregisterdata"
  val APPMASTER_CONFIG = "applicationMasterConfig"

  val GEARPUMP_SCHEDULING_SCHEDULER = "gearpump.scheduling.scheduler-class"
  val GEARPUMP_SCHEDULING_REQUEST = "gearpump.scheduling.requests"

  val GEARPUMP_SERIALIZERS = "gearpump.serializers"

  val GEARPUMP_TASK_DISPATCHER = "gearpump.task-dispatcher"
  val GEARPUMP_CLUSTER_MASTERS = "gearpump.cluster.masters"

  val GEARPUMP_APPMASTER_ARGS = "gearpump.appmaster.vmargs"
  val GEARPUMP_APPMASTER_EXTRA_CLASSPATH = "gearpump.appmaster.extraClasspath"

  val GEARPUMP_EXECUTOR_ARGS = "gearpump.executor.vmargs"
  val GEARPUMP_EXECUTOR_EXTRA_CLASSPATH = "gearpump.executor.extraClasspath"

  val GEARPUMP_LOG_DAEMON_DIR = "gearpump.log.daemon.dir"
  val GEARPUMP_LOG_APPLICATION_DIR = "gearpump.log.application.dir"

  //config for construction of executor
  val APP_MASTER = "appMaster"
  val APP_JAR = "appJar"
  val EXECUTOR_ID = "executorId"
  val RESOURCE = "resource"

  val TASK_ID = "taskId"
  val TASK_DAG = "taskDag"

  val TASK_CONFIG = "taskConfig"

  val START_TIME = "startTime"

  val HADOOP_CONF = "hadoopConf"

  //the time out for Future
  val FUTURE_TIMEOUT = akka.util.Timeout(5, TimeUnit.SECONDS)

  val NETTY_BUFFER_SIZE = "gearpump.netty.buffer-size"
  val NETTY_MAX_RETRIES = "gearpump.netty.max-retries"
  val NETTY_BASE_SLEEP_MS = "gearpump.netty.base-sleep-ms"
  val NETTY_MAX_SLEEP_MS = "gearpump.netty.max-sleep-ms"
  val NETTY_MESSAGE_BATCH_SIZE = "gearpump.netty.message-batch-size"
  val NETTY_FLUSH_CHECK_INTERVAL = "gearpump.netty.fulsh-check-interval"

  val NETTY_TCP_HOSTNAME = "akka.remote.netty.tcp.hostname"
  val GEAR_USERNAME = "gearpump.username"
  val GEAR_APPLICATION_ID = "gearpump.applicationId"
  val GEAR_EXECUTOR_ID = "gearpump.executorId"
  // Application jar property
  val GEAR_APP_JAR = "gearpump.app.jar"

  val GEAR_APP_JAR_STORE_ROOT_PATH = "gearpump.jarstore.rootpath"

  // Use java property -Dconfig.file=xxx.conf to set customized configuration
  // Otherwise application.conf in classpath will be loaded
  val CUSTOM_CONFIG_FILE = "config.file"
}
