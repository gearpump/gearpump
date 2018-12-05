/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.util

import java.util.concurrent.TimeUnit

object Constants {
  val MASTER_WATCHER = "masterwatcher"
  val SINGLETON_MANAGER = "singleton"

  val MASTER_CONFIG = "gearpump-master"
  val WORKER_CONFIG = "gearpump-worker"
  val UI_CONFIG = "gearpump-ui"
  val LINUX_CONFIG = "gearpump-linux" // linux or Mac

  val MASTER = "master"
  val WORKER = "worker"

  val GEARPUMP_WORKER_SLOTS = "gearpump.worker.slots"
  val GEARPUMP_EXECUTOR_PROCESS_LAUNCHER = "gearpump.worker.executor-process-launcher"
  val GEARPUMP_SCHEDULING_SCHEDULER = "gearpump.scheduling.scheduler-class"
  val GEARPUMP_SCHEDULING_REQUEST = "gearpump.scheduling.requests"
  val GEARPUMP_TRANSPORT_SERIALIZER = "gearpump.transport.serializer"
  val GEARPUMP_SERIALIZER_POOL = "gearpump.serialization-framework"
  val GEARPUMP_SERIALIZERS = "gearpump.serializers"
  val GEARPUMP_TASK_DISPATCHER = "gearpump.task-dispatcher"
  val GEARPUMP_CLUSTER_MASTERS = "gearpump.cluster.masters"
  val GEARPUMP_MASTERCLIENT_TIMEOUT = "gearpump.masterclient.timeout"
  val GEARPUMP_CLUSTER_EXECUTOR_WORKER_SHARE_SAME_PROCESS =
    "gearpump.worker.executor-share-same-jvm-as-worker"

  val GEARPUMP_HOME = "gearpump.home"
  val GEARPUMP_FULL_SCALA_VERSION = "gearpump.binary-version-with-scala-version"
  val GEARPUMP_HOSTNAME = "gearpump.hostname"
  val GEARPUMP_APPMASTER_ARGS = "gearpump.appmaster.vmargs"
  val GEARPUMP_APPMASTER_EXTRA_CLASSPATH = "gearpump.appmaster.extraClasspath"
  val GEARPUMP_EXECUTOR_ARGS = "gearpump.executor.vmargs"
  val GEARPUMP_EXECUTOR_EXTRA_CLASSPATH = "gearpump.executor.extraClasspath"
  val GEARPUMP_LOG_DAEMON_DIR = "gearpump.log.daemon.dir"
  val GEARPUMP_LOG_APPLICATION_DIR = "gearpump.log.application.dir"
  val HADOOP_CONF = "hadoopConf"

  // Id used to identity Master JVM process in low level resource manager like YARN.
  // In YARN, it means the container Id.
  val GEARPUMP_MASTER_RESOURCE_MANAGER_CONTAINER_ID =
    "gearpump.master-resource-manager-container-id"

  // Id used to identity Worker JVM process in low level resource manager like YARN.
  // In YARN, it means the container Id.
  val GEARPUMP_WORKER_RESOURCE_MANAGER_CONTAINER_ID =
    "gearpump.worker-resource-manager-container-id"

  // true or false
  val GEARPUMP_REMOTE_DEBUG_EXECUTOR_JVM = "gearpump.remote-debug-executor-jvm"
  val GEARPUMP_REMOTE_DEBUG_PORT = "gearpump.remote-debug-port"

  // Whether to turn on GC log, true or false
  val GEARPUMP_VERBOSE_GC = "gearpump.verbose-gc"

  // The time out for Future, like ask.
  // !Important! This global timeout setting will also impact the UI
  // responsive time if set to too big. Please make sure you have
  // enough justification to change this global setting, otherwise
  // please use your local timeout setting instead.
  val FUTURE_TIMEOUT = akka.util.Timeout(15, TimeUnit.SECONDS)

  val GEARPUMP_START_EXECUTOR_SYSTEM_TIMEOUT_MS = "gearpump.start-executor-system-timeout-ms"

  val APPMASTER_DEFAULT_EXECUTOR_ID = -1

  val NETTY_BUFFER_SIZE = "gearpump.netty.buffer-size"
  val NETTY_MAX_RETRIES = "gearpump.netty.max-retries"
  val NETTY_BASE_SLEEP_MS = "gearpump.netty.base-sleep-ms"
  val NETTY_MAX_SLEEP_MS = "gearpump.netty.max-sleep-ms"
  val NETTY_MESSAGE_BATCH_SIZE = "gearpump.netty.message-batch-size"
  val NETTY_FLUSH_CHECK_INTERVAL = "gearpump.netty.flush-check-interval"
  val NETTY_TCP_HOSTNAME = "akka.remote.netty.tcp.hostname"
  val NETTY_DISPATCHER = "gearpump.netty.dispatcher"

  val GEARPUMP_USERNAME = "gearpump.username"
  val GEARPUMP_APPLICATION_ID = "gearpump.applicationId"
  val GEARPUMP_MASTER_STARTTIME = "gearpump.master.starttime"
  val GEARPUMP_EXECUTOR_ID = "gearpump.executorId"
  // Application jar property
  val GEARPUMP_APP_JAR = "gearpump.app.jar"
  val GEARPUMP_APP_NAME_PREFIX = "gearpump.app.name.prefix"

  // Where the jar is stored at. It can be a HDFS, or a local disk.
  val GEARPUMP_APP_JAR_STORE_ROOT_PATH = "gearpump.jarstore.rootpath"

  // Uses java property -Dgearpump.config.file=xxx.conf to set customized configuration
  // Otherwise application.conf in classpath will be loaded
  val GEARPUMP_CUSTOM_CONFIG_FILE = "gearpump.config.file"

  // Metrics related
  val GEARPUMP_METRIC_ENABLED = "gearpump.metrics.enabled"
  val GEARPUMP_METRIC_SAMPLE_RATE = "gearpump.metrics.sample-rate"
  val GEARPUMP_METRIC_REPORT_INTERVAL = "gearpump.metrics.report-interval-ms"
  val GEARPUMP_METRIC_GRAPHITE_HOST = "gearpump.metrics.graphite.host"
  val GEARPUMP_METRIC_GRAPHITE_PORT = "gearpump.metrics.graphite.port"
  val GEARPUMP_METRIC_REPORTER = "gearpump.metrics.reporter"

  // Retains at max @RETAIN_HISTORY_HOURS history data
  val GEARPUMP_METRIC_RETAIN_HISTORY_DATA_HOURS = "gearpump.metrics.retainHistoryData.hours"

  // Time interval between two history data points.
  val GEARPUMP_RETAIN_HISTORY_DATA_INTERVAL_MS = "gearpump.metrics.retainHistoryData.intervalMs"

  // Retains at max @RETAIN_LATEST_SECONDS recent data points
  val GEARPUMP_RETAIN_RECENT_DATA_SECONDS = "gearpump.metrics.retainRecentData.seconds"

  // time interval between two recent data points.
  val GEARPUMP_RETAIN_RECENT_DATA_INTERVAL_MS = "gearpump.metrics.retainRecentData.intervalMs"

  // AppMaster will max wait this time until it declare the resource cannot be allocated,
  // and shutdown itself
  val GEARPUMP_RESOURCE_ALLOCATION_TIMEOUT = "gearpump.resource-allocation-timeout-seconds"

  // Service related
  val GEARPUMP_SERVICE_HTTP = "gearpump.services.http"
  val GEARPUMP_SERVICE_HOST = "gearpump.services.host"
  val GEARPUMP_SERVICE_SUPERVISOR_PATH = "gearpump.services.supervisor-actor-path"
  val GEARPUMP_SERVICE_RENDER_CONFIG_CONCISE = "gearpump.services.config-render-option-concise"

  // Security related
  val GEARPUMP_KEYTAB_FILE = "gearpump.keytab.file"
  val GEARPUMP_KERBEROS_PRINCIPAL = "gearpump.kerberos.principal"

  val GEARPUMP_METRICS_MAX_LIMIT = "gearpump.metrics.akka.max-limit-on-query"
  val GEARPUMP_METRICS_AGGREGATORS = "gearpump.metrics.akka.metrics-aggregator-class"

  val GEARPUMP_UI_SECURITY = "gearpump.ui-security"
  val GEARPUMP_UI_SECURITY_AUTHENTICATION_ENABLED = "gearpump.ui-security.authentication-enabled"
  val GEARPUMP_UI_AUTHENTICATOR_CLASS = "gearpump.ui-security.authenticator"
  // OAuth Authentication Factory for UI server.
  val GEARPUMP_UI_OAUTH2_AUTHENTICATOR_ENABLED = "gearpump.ui-security.oauth2-authenticator-enabled"
  val GEARPUMP_UI_OAUTH2_AUTHENTICATORS = "gearpump.ui-security.oauth2-authenticators"
  val GEARPUMP_UI_OAUTH2_AUTHENTICATOR_CLASS = "class"
  val GEARPUMP_UI_OAUTH2_AUTHENTICATOR_CALLBACK = "callback"
  val GEARPUMP_UI_OAUTH2_AUTHENTICATOR_CLIENT_ID = "clientid"
  val GEARPUMP_UI_OAUTH2_AUTHENTICATOR_CLIENT_SECRET = "clientsecret"
  val GEARPUMP_UI_OAUTH2_AUTHENTICATOR_DEFAULT_USER_ROLE = "default-userrole"
  val GEARPUMP_UI_OAUTH2_AUTHENTICATOR_AUTHORIZATION_CODE = "code"
  val GEARPUMP_UI_OAUTH2_AUTHENTICATOR_ACCESS_TOKEN = "accesstoken"

  val PREFER_IPV4 = "java.net.preferIPv4Stack"

  val APPLICATION_EXECUTOR_NUMBER = "gearpump.application.executor-num"

  val APPLICATION_TOTAL_RETRIES = "gearpump.application.total-retries"

  val AKKA_SCHEDULER_TICK_DURATION = "akka.scheduler.tick-duration"
}
