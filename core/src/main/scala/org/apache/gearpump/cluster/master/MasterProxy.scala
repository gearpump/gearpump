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

package org.apache.gearpump.cluster.master

import java.util.concurrent.TimeUnit

import akka.actor._
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{ActorUtil, LogUtil}
import org.slf4j.Logger

import scala.concurrent.duration.{FiniteDuration, Duration}

/**
 * This works with Master HA. When there are multiple Master nodes,
 * This will find a active one.
 *
 *
 * @param masters
 */
class MasterProxy (masters: Iterable[ActorPath])
  extends Actor with Stash {
  import MasterProxy._

  val LOG: Logger = LogUtil.getLogger(getClass)

  val contacts = masters.map { url =>
    LOG.info(s"Contacts point URL: $url")
    context.actorSelection(url)
  }

  var watchers: List[ActorRef] = List.empty[ActorRef]

  import context.dispatcher

  def findMaster() = repeatActionUtil(30){
    contacts foreach { contact =>
      LOG.info(s"sending identity to $contact")
      contact ! Identify(None)
    }
  }

  context.become(establishing(findMaster()))

  LOG.info("Master Proxy is started...")

  override def postStop(): Unit = {
    watchers.foreach(_ ! MasterStopped)
    super.postStop()
  }

  override def receive : Receive = {
    case _=>
  }

  def establishing(cancelFindMaster : Cancellable): Actor.Receive = {
    case ActorIdentity(_, Some(receptionist)) =>
      context watch receptionist
      LOG.info("Connected to [{}]", receptionist.path)
      context.watch(receptionist)

      watchers.foreach(_ ! MasterRestarted)
      unstashAll()
      cancelFindMaster.cancel()
      context.become(active(receptionist) orElse messageHandler(receptionist))
    case ActorIdentity(_, None) => // ok, use another instead
    case msg =>
      LOG.info(s"get unknown message , stashing ${msg.getClass.getSimpleName}")
      stash()
  }

  def active(receptionist: ActorRef): Actor.Receive = {
    case Terminated(receptionist) ⇒
      LOG.info("Lost contact with [{}], restablishing connection", receptionist)
      context.become(establishing(findMaster))
    case _: ActorIdentity ⇒ // ok, from previous establish, already handled
    case WatchMaster(watcher) =>
      watchers = watchers :+ watcher
  }

  def messageHandler(master: ActorRef): Receive = {
    case msg =>
      LOG.debug(s"Get msg ${msg.getClass.getSimpleName}, forwarding to ${master.path}")
      master forward msg
  }

  def scheduler = context.system.scheduler
  import scala.concurrent.duration._
  private def repeatActionUtil(seconds: Int)(action : => Unit) : Cancellable = {
    val cancelSend = scheduler.schedule(0 seconds, 2 seconds)(action)
    val cancelSuicide = scheduler.scheduleOnce(seconds seconds, self, PoisonPill)
    new Cancellable {
      def cancel(): Boolean = {
        val result1 = cancelSend.cancel()
        val result2 = cancelSuicide.cancel()
        result1 && result2
      }

      def isCancelled: Boolean = {
        cancelSend.isCancelled && cancelSuicide.isCancelled
      }
    }
  }
}

object MasterProxy {
  case object MasterRestarted
  case object MasterStopped
  case class WatchMaster(watcher: ActorRef)

  def props(masters: Iterable[HostPort]): Props = {
    val contacts = masters.map(ActorUtil.getMasterActorPath(_))
    Props(new MasterProxy(contacts))
  }
}