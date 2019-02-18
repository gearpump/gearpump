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

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.DistributedData
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import com.typesafe.config.ConfigValueFactory
import io.gearpump.cluster.ClusterConfig
import io.gearpump.util.{AkkaApp, Constants, LogUtil}
import io.gearpump.util.Constants._
import io.gearpump.util.LogUtil.ProcessType
import java.util.concurrent.TimeUnit
import org.slf4j.Logger
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Master extends AkkaApp with ArgumentsParser {

  private var LOG: Logger = LogUtil.getLogger(getClass)

  override def akkaConfig: Config = ClusterConfig.master()

  override val options: Array[(String, CLIOption[Any])] =
    Array("ip" -> CLIOption[String]("<master ip address>", required = true),
      "port" -> CLIOption("<master port>", required = true))

  override val description = "Start Master daemon"

  def main(akkaConf: Config, args: Array[String]): Unit = {

    this.LOG = {
      LogUtil.loadConfiguration(akkaConf, ProcessType.MASTER)
      LogUtil.getLogger(getClass)
    }

    val config = parse(args)
    master(config.getString("ip"), config.getInt("port"), akkaConf)
  }

  private def verifyMaster(master: String, port: Int, masters: Iterable[String]) = {
    masters.exists { hostPort =>
      hostPort == s"$master:$port"
    }
  }

  private def master(ip: String, port: Int, akkaConf: Config): Unit = {
    val masters = akkaConf.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).asScala

    if (!verifyMaster(ip, port, masters)) {
      LOG.error(s"The provided ip $ip and port $port doesn't conform with config at " +
        s"gearpump.cluster.masters: ${masters.mkString(", ")}")
      System.exit(-1)
    }

    val masterList = masters.map(master => s"akka.tcp://${MASTER}@$master").toList.asJava
    val quorum = masterList.size() / 2 + 1
    val masterConfig = akkaConf.
      withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(port)).
      withValue(NETTY_TCP_HOSTNAME, ConfigValueFactory.fromAnyRef(ip)).
      withValue("akka.cluster.seed-nodes", ConfigValueFactory.fromAnyRef(masterList)).
      withValue(s"akka.cluster.role.${MASTER}.min-nr-of-members",
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
