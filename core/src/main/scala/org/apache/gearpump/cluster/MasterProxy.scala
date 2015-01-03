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

package org.apache.gearpump.cluster

import java.util.concurrent.TimeUnit

import akka.actor._
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.LogUtil
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.Duration

class MasterProxy(masters: Iterable[HostPort])
  extends Actor with Stash {

  private val LOG: Logger = LogUtil.getLogger(getClass)

  val contacts = masters.map { master =>
    s"akka.tcp://$MASTER@${master.host}:${master.port}/user/$MASTER"
  }.map { url =>
    LOG.info(s"Contacts point URL: $url")
    context.actorSelection(url)
  }


  import context.dispatcher

  def findMaster() = context.system.scheduler.schedule(Duration.Zero, Duration(1, TimeUnit.SECONDS)){
    contacts foreach { contact =>
      LOG.info(s"sending identity to $contact")
      contact ! Identify(None)
    }
  }

  context.become(establishing(findMaster()))

  LOG.info("Master Proxy is started...")

  override def postStop(): Unit = {
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
      unstashAll()
      cancelFindMaster.cancel()
      context.become(active(receptionist))
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
    case msg =>
      LOG.info(s"Get msg ${msg.getClass.getSimpleName}, forwarding to ${receptionist.path}")
      receptionist forward msg
  }
}