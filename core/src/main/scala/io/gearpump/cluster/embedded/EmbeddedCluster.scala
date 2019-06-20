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

package io.gearpump.cluster.embedded

import akka.actor.{ActorRef, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigValueFactory}
import io.gearpump.cluster.ClusterConfig
import io.gearpump.cluster.master.Master
import io.gearpump.cluster.worker.{Worker => WorkerActor}
import io.gearpump.util.{LogUtil, Util}
import io.gearpump.util.Constants.{GEARPUMP_CLUSTER_EXECUTOR_WORKER_SHARE_SAME_PROCESS, GEARPUMP_CLUSTER_MASTERS, GEARPUMP_METRIC_ENABLED, MASTER}
import scala.collection.JavaConverters._

/**
 * Create a in-process cluster with single worker
 */
class EmbeddedCluster(inputConfig: Config) {
  private val LOG = LogUtil.getLogger(getClass)
  private val workerCount: Int = 1
  private val port = Util.findFreePort().get

  val config: Config = getConfig(inputConfig, port)
  val system: ActorSystem = ActorSystem(MASTER, config)
  val master: ActorRef = system.actorOf(Props[Master], MASTER)

  0.until(workerCount).foreach { id =>
    system.actorOf(Props(classOf[WorkerActor], master), classOf[WorkerActor].getSimpleName + id)
  }

  LOG.info("=================================")
  LOG.info("Local Cluster is started at: ")
  LOG.info(s"                 127.0.0.1:$port")
  LOG.info(s"To see UI, run command: services -master 127.0.0.1:$port")

  private def getConfig(inputConfig: Config, port: Int): Config = {
    val config = inputConfig.
      withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(port)).
      withValue(GEARPUMP_CLUSTER_MASTERS,
        ConfigValueFactory.fromIterable(List(s"127.0.0.1:$port").asJava)).
      withValue(GEARPUMP_CLUSTER_EXECUTOR_WORKER_SHARE_SAME_PROCESS,
        ConfigValueFactory.fromAnyRef(true)).
      withValue(GEARPUMP_METRIC_ENABLED, ConfigValueFactory.fromAnyRef(true)).
      withValue("akka.actor.provider",
        ConfigValueFactory.fromAnyRef("akka.cluster.ClusterActorRefProvider"))
    config
  }
}

object EmbeddedCluster {
  def apply(): EmbeddedCluster = {
    new EmbeddedCluster(ClusterConfig.master())
  }
}