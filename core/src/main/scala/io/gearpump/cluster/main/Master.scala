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

package io.gearpump.cluster.main

import org.apache.pekko.actor.{ActorSystem, PoisonPill, Props}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.cluster.ddata.DistributedData
import org.apache.pekko.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import com.typesafe.config.ConfigValueFactory
import io.gearpump.cluster.ClusterConfig
import io.gearpump.util.{PekkoApp, Constants, LogUtil}
import io.gearpump.util.Constants._
import io.gearpump.util.LogUtil.ProcessType
import java.util.concurrent.TimeUnit
import org.slf4j.Logger
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Master extends PekkoApp with ArgumentsParser {

  private var LOG: Logger = LogUtil.getLogger(getClass)

  override def pekkoConfig: Config = ClusterConfig.master()

  override val options: Array[(String, CLIOption[Any])] =
    Array("ip" -> CLIOption[String]("<master ip address>", required = true),
      "port" -> CLIOption("<master port>", required = true))

  override val description = "Start Master daemon"

  def main(pekkoConf: Config, args: Array[String]): Unit = {

    this.LOG = {
      LogUtil.loadConfiguration(pekkoConf, ProcessType.MASTER)
      LogUtil.getLogger(getClass)
    }

    val config = parse(args)
    master(config.getString("ip"), config.getInt("port"), pekkoConf)
  }

  private def verifyMaster(master: String, port: Int, masters: Iterable[String]) = {
    masters.exists { hostPort =>
      hostPort == s"$master:$port"
    }
  }

  private def master(ip: String, port: Int, pekkoConf: Config): Unit = {
    val masters = pekkoConf.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).asScala

    if (!verifyMaster(ip, port, masters)) {
      LOG.error(s"The provided ip $ip and port $port doesn't conform with config at " +
        s"gearpump.cluster.masters: ${masters.mkString(", ")}")
      System.exit(-1)
    }

    val masterList = masters.map(master => s"pekko.tcp://${MASTER}@$master").toList.asJava
    val quorum = masterList.size() / 2 + 1
    val masterConfig = pekkoConf.
      withValue("pekko.remote.classic.netty.tcp.port", ConfigValueFactory.fromAnyRef(port)).
      withValue(NETTY_TCP_HOSTNAME, ConfigValueFactory.fromAnyRef(ip)).
      withValue("pekko.cluster.seed-nodes", ConfigValueFactory.fromAnyRef(masterList)).
      withValue(s"pekko.cluster.role.${MASTER}.min-nr-of-members",
        ConfigValueFactory.fromAnyRef(quorum))

    LOG.info(s"Starting Master Actor system $ip:$port, master list: ${masters.mkString(";")}")
    val system = ActorSystem(MASTER, masterConfig)

    val replicator = DistributedData(system).replicator
    LOG.info(s"Replicator path: ${replicator.path}")

    // Starts singleton manager
    val _ = system.actorOf(ClusterSingletonManager.props(
      singletonProps = Props(classOf[MasterWatcher], MASTER),
      terminationMessage = PoisonPill,
      settings = ClusterSingletonManagerSettings(system).withSingletonName(MASTER_WATCHER)
        .withRole(MASTER)),
      name = SINGLETON_MANAGER)

    // Start master proxy
    val masterProxy = system.actorOf(ClusterSingletonProxy.props(
      singletonManagerPath = s"/user/${SINGLETON_MANAGER}",
      // The effective singleton is s"${MASTER_WATCHER}/$MASTER" instead of s"${MASTER_WATCHER}".
      // Master is created when there is a majority of machines started.
      settings = ClusterSingletonProxySettings(system)
        .withSingletonName(s"${MASTER_WATCHER}/$MASTER").withRole(MASTER)),
      name = MASTER
    )

    LOG.info(s"master proxy is started at ${masterProxy.path}")

    val mainThread = Thread.currentThread()
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run(): Unit = {
        if (!system.whenTerminated.isCompleted) {
          LOG.info("Triggering shutdown hook....")

          system.stop(masterProxy)
          val cluster = Cluster(system)
          cluster.leave(cluster.selfAddress)
          cluster.down(cluster.selfAddress)
          try {
            Await.result(system.whenTerminated, Duration(3, TimeUnit.SECONDS))
          } catch {
            case _: Exception => // Ignore
          }
          system.terminate()
          mainThread.join()
        }
      }
    })

    Await.result(system.whenTerminated, Duration.Inf)
  }
}
