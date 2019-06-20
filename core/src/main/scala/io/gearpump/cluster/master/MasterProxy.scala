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

package io.gearpump.cluster.master

import akka.actor._
import io.gearpump.transport.HostPort
import io.gearpump.util.{ActorUtil, LogUtil}
import org.slf4j.Logger
import scala.concurrent.duration.FiniteDuration

/**
 * This works with Master HA. When there are multiple Master nodes,
 * This will find a active one.
 */
class MasterProxy(masters: Iterable[ActorPath], timeout: FiniteDuration)
  extends Actor with Stash {
  import io.gearpump.cluster.master.MasterProxy._

  private val LOG: Logger = LogUtil.getLogger(getClass, name = self.path.name)

  private val contacts = masters.map { url =>
    LOG.info(s"Contacts point URL: $url")
    context.actorSelection(url)
  }

  private var watchers: List[ActorRef] = List.empty[ActorRef]

  import context.dispatcher

  def findMaster(): Cancellable = {
    repeatActionUtil(timeout) {
      contacts foreach { contact =>
        LOG.info(s"sending identity to $contact")
        contact ! Identify(None)
      }
    }
  }

  context.become(establishing(findMaster()))

  LOG.info("Master Proxy is started...")

  override def postStop(): Unit = {
    watchers.foreach(_ ! MasterStopped)
    super.postStop()
  }

  override def receive: Receive = {
    case _ =>
  }

  def establishing(findMaster: Cancellable): Receive = {
    case ActorIdentity(_, Some(receptionist)) =>
      context watch receptionist
      LOG.info(s"Connected to [${receptionist.path}]")
      context.watch(receptionist)

      watchers.foreach(_ ! MasterRestarted)
      unstashAll()
      findMaster.cancel()
      context.become(active orElse messageHandler(receptionist))
    case ActorIdentity(_, None) => // ok, use another instead
    case msg =>
      LOG.debug(s"Stashing ${msg.getClass.getSimpleName}")
      stash()
  }

  def active: Receive = {
    case Terminated(master) =>
      LOG.warn(s"Lost contact with [$master], reestablishing connection")
      context.become(establishing(findMaster()))
    case _: ActorIdentity => // ok, from previous establish, already handled
    case WatchMaster(watcher) =>
      watchers = watchers :+ watcher
  }

  def messageHandler(master: ActorRef): Receive = {
    case msg =>
      LOG.debug(s"Get msg ${msg.getClass.getSimpleName}, forwarding to ${master.path}")
      master forward msg
  }

  def scheduler: Scheduler = context.system.scheduler
  import scala.concurrent.duration._
  private def repeatActionUtil(timeout: FiniteDuration)(action: => Unit): Cancellable = {
    val send = scheduler.schedule(0.seconds, 2.seconds)(action)
    val suicide = scheduler.scheduleOnce(timeout) {
      send.cancel()
      self ! PoisonPill
    }

    new Cancellable {
      def cancel(): Boolean = {
        val result1 = send.cancel()
        val result2 = suicide.cancel()
        result1 && result2
      }

      def isCancelled: Boolean = {
        send.isCancelled && suicide.isCancelled
      }
    }
  }
}

object MasterProxy {
  case object MasterRestarted
  case object MasterStopped
  case class WatchMaster(watcher: ActorRef)

  import scala.concurrent.duration._
  def props(masters: Iterable[HostPort], duration: FiniteDuration = 30.seconds): Props = {
    val contacts = masters.map(ActorUtil.getMasterActorPath)
    Props(new MasterProxy(contacts, duration))
  }
}