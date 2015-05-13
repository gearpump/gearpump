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

package gearpump.cluster.main

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.contrib.datareplication.DataReplication
import akka.contrib.pattern.{ClusterSingletonManager, ClusterSingletonProxy}
import com.typesafe.config.ConfigValueFactory
import gearpump.cluster.ClusterConfig
import gearpump.cluster.master.{Master => MasterActor}
import gearpump.util.Constants._
import gearpump.util.LogUtil.ProcessType
import gearpump.util.{Constants, LogUtil}
import org.slf4j.Logger

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.duration._
object Master extends App with ArgumentsParser {
  var masterConfig = ClusterConfig.load.master
  private val LOG: Logger = {
    LogUtil.loadConfiguration(masterConfig, ProcessType.MASTER)
    LogUtil.getLogger(getClass)
  }

  val options: Array[(String, CLIOption[Any])] = 
    Array("ip"->CLIOption[String]("<master ip address>",required = true),
      "port"->CLIOption("<master port>",required = true))


  val config = parse(args)

  def start() = {
    master(config.getString("ip"), config.getInt("port"))
  }

  def verifyMaster(master : String, port: Int, masters : Iterable[String])  = {
    masters.exists{ hostPort =>
      hostPort == s"$master:$port"
    }
  }

  def master(ip:String, port : Int): Unit = {
    val masters = masterConfig.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).asScala

    if (!verifyMaster(ip, port, masters)) {
      LOG.error(s"The provided ip $ip and port $port doesn't conform with config at gearpump.cluster.masters: ${masters.mkString(", ")}")
      System.exit(-1)
    }

    val masterList = masters.map(master => s"akka.tcp://${MASTER}@$master").toList.asJava
    val quorum = masterList.size() /2  + 1
    masterConfig = masterConfig.
      withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(port)).
      withValue(NETTY_TCP_HOSTNAME, ConfigValueFactory.fromAnyRef(ip)).
      withValue("akka.cluster.seed-nodes", ConfigValueFactory.fromAnyRef(masterList)).
      withValue(s"akka.cluster.role.${MASTER}.min-nr-of-members", ConfigValueFactory.fromAnyRef(quorum))

    LOG.info(s"Starting Master Actor system $ip:$port, master list: ${masters.mkString(";")}")
    val system = ActorSystem(MASTER, masterConfig)

    val replicator = DataReplication(system).replicator
    LOG.info(s"Replicator path: ${replicator.path}")

    //start master proxy
    val masterProxy = system.actorOf(ClusterSingletonProxy.props(
      singletonPath = s"/user/${SINGLETON_MANAGER}/${MASTER_WATCHER}/${MASTER}",
      role = Some(MASTER)),
      name = MASTER)

    //start singleton manager
    val singletonManager = system.actorOf(ClusterSingletonManager.props(
      singletonProps = Props(classOf[MasterWatcher], MASTER, masterProxy),
      singletonName = MASTER_WATCHER,
      terminationMessage = PoisonPill,
      role = Some(MASTER)),
      name = SINGLETON_MANAGER)

    LOG.info(s"master proxy is started at ${masterProxy.path}")

    val mainThread = Thread.currentThread();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run() : Unit = {
        if (!system.isTerminated) {
          LOG.info("Triggering shutdown hook....")

          system.stop(masterProxy)
          val cluster = Cluster(system)
          cluster.leave(cluster.selfAddress)
          cluster.down(cluster.selfAddress)
          try {
            system.awaitTermination(Duration(3, TimeUnit.SECONDS))
          } catch {
            case ex : Exception => //ignore
          }
          system.shutdown()
          mainThread.join();
        }
      }
    });

    system.awaitTermination()
  }

  start()
}

class MasterWatcher(role: String, masterProxy : ActorRef) extends Actor  with ActorLogging {
  import context.dispatcher

  val cluster = Cluster(context.system)

  val config = context.system.settings.config
  val masters = config.getList("akka.cluster.seed-nodes")
  val quorum = masters.size() / 2 + 1

  val system = context.system

  // sort by age, oldest first
  val ageOrdering = Ordering.fromLessThan[Member] { (a, b) => a.isOlderThan(b) }
  var membersByAge: immutable.SortedSet[Member] = immutable.SortedSet.empty(ageOrdering)

  def receive : Receive = null

  // subscribe to MemberEvent, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent])
    context.become(waitForInit)
  }
  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  def matchingRole(member: Member): Boolean = member.hasRole(role)

  def waitForInit : Receive = {
    case state: CurrentClusterState => {
      membersByAge = immutable.SortedSet.empty(ageOrdering) ++ state.members.filter(m =>
        m.status == MemberStatus.Up && matchingRole(m))

      if (membersByAge.size < quorum) {
        membersByAge.iterator.mkString(",")
        log.info(s"We cannot get a quorum, $quorum, shutting down...${membersByAge.iterator.mkString(",")}")
        context.become(waitForShutdown)
        self ! MasterWatcher.Shutdown
      } else {
        context.actorOf(Props(classOf[MasterActor]), MASTER)
        context.become(waitForClusterEvent)
      }
    }
  }

  def waitForClusterEvent : Receive = {
    case MemberUp(m) if matchingRole(m)  => {
      membersByAge += m
    }
    case mEvent: MemberEvent if (mEvent.isInstanceOf[MemberExited] || mEvent.isInstanceOf[MemberRemoved]) && matchingRole(mEvent.member) => {
      log.info(s"member removed ${mEvent.member}")
      val m = mEvent.member
      membersByAge -= m
      if (membersByAge.size < quorum) {
        log.info(s"We cannot get a quorum, $quorum, shutting down...${membersByAge.iterator.mkString(",")}")
        context.become(waitForShutdown)
        self ! MasterWatcher.Shutdown
      }
    }
  }

  def waitForShutdown : Receive = {
    case MasterWatcher.Shutdown => {
      context.system.stop(masterProxy)
      cluster.unsubscribe(self)
      cluster.leave(cluster.selfAddress)
      context.stop(self)
      system.scheduler.scheduleOnce(Duration.Zero) {
        try {
          system.awaitTermination(Duration(3, TimeUnit.SECONDS))
        } catch {
          case ex : Exception => //ignore
        }
        system.shutdown()
      }
    }
  }
}

object MasterWatcher {
  object Shutdown
}

