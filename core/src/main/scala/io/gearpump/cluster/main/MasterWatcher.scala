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

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberEvent, MemberExited, MemberRemoved, MemberUp}
import io.gearpump.cluster.master.{Master => MasterActor, MasterNode}
import io.gearpump.cluster.master.Master.MasterListUpdated
import io.gearpump.util.Constants.MASTER
import java.util.concurrent.TimeUnit
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class MasterWatcher(role: String) extends Actor with ActorLogging {
  import context.dispatcher

  val cluster = Cluster(context.system)

  val config = context.system.settings.config
  val masters = config.getList("akka.cluster.seed-nodes")
  val quorum = masters.size() / 2 + 1

  val system = context.system

  // Sorts by age, oldest first
  val ageOrdering = Ordering.fromLessThan[Member] { (a, b) => a.isOlderThan(b) }
  var membersByAge: immutable.SortedSet[Member] = immutable.SortedSet.empty(ageOrdering)

  def receive: Receive = null

  // Subscribes to MemberEvent, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent])
    context.become(waitForInit)
  }
  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  def matchingRole(member: Member): Boolean = member.hasRole(role)

  def waitForInit: Receive = {
    case state: CurrentClusterState => {
      membersByAge = immutable.SortedSet.empty(ageOrdering) ++ state.members.filter(m =>
        m.status == MemberStatus.Up && matchingRole(m))

      if (membersByAge.size < quorum) {
        membersByAge.iterator.mkString(",")
        log.info(s"We cannot get a quorum, $quorum, " +
          s"shutting down...${membersByAge.iterator.mkString(",")}")
        context.become(waitForShutdown)
        self ! MasterWatcher.Shutdown
      } else {
        val master = context.actorOf(Props(classOf[MasterActor]), MASTER)
        notifyMasterMembersChange(master)
        context.become(waitForClusterEvent(master))
      }
    }
  }

  def waitForClusterEvent(master: ActorRef): Receive = {
    case MemberUp(m) if matchingRole(m) => {
      membersByAge += m
      notifyMasterMembersChange(master)
    }
    case mEvent: MemberEvent if (mEvent.isInstanceOf[MemberExited] ||
      mEvent.isInstanceOf[MemberRemoved]) && matchingRole(mEvent.member) => {
      log.info(s"member removed ${mEvent.member}")
      val m = mEvent.member
      membersByAge -= m
      if (membersByAge.size < quorum) {
        log.info(s"We cannot get a quorum, $quorum, " +
          s"shutting down...${membersByAge.iterator.mkString(",")}")
        context.become(waitForShutdown)
        self ! MasterWatcher.Shutdown
      } else {
        notifyMasterMembersChange(master)
      }
    }
  }

  private def notifyMasterMembersChange(master: ActorRef): Unit = {
    val masters = membersByAge.toList.map{ member =>
      MasterNode(member.address.host.getOrElse("Unknown-Host"),
        member.address.port.getOrElse(0))
    }
    master ! MasterListUpdated(masters)
  }

  def waitForShutdown: Receive = {
    case MasterWatcher.Shutdown => {
      cluster.unsubscribe(self)
      cluster.leave(cluster.selfAddress)
      context.stop(self)
      system.scheduler.scheduleOnce(Duration.Zero) {
        try {
          Await.result(system.whenTerminated, Duration(3, TimeUnit.SECONDS))
        } catch {
          case _: Exception => // Ignore
        }
        system.terminate()
      }
    }
  }
}

object MasterWatcher {
  object Shutdown
}