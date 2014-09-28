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

import akka.actor._
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Configs
import org.apache.gearpump.util.Constants._
import org.slf4j.{LoggerFactory, Logger}

class MasterProxy(masters: Iterable[HostPort])
  extends Actor with Stash {

  import MasterProxy._

  val contacts = masters.map { master =>
    s"akka.tcp://${MASTER}@${master.host}:${master.port}/user/${MASTER}"
  }.map { url =>
    context.actorSelection(url)
  }

  contacts foreach { _ ! Identify(None) }

  LOG.info("Master Proxy is started...")

  override def postStop(): Unit = {
    super.postStop()
  }

  def receive = establishing

  def establishing: Actor.Receive = {
    case ActorIdentity(_, Some(receptionist)) =>
      context watch receptionist
      LOG.info("Connected to [{}]", receptionist.path)
      context.watch(receptionist)
      unstashAll()
      context.become(active(receptionist))
    case ActorIdentity(_, None) => // ok, use another instead
    case msg =>
      LOG.info(s"get unknown message , stashing ${msg.getClass.getSimpleName}")
      stash()
  }

  def active(receptionist: ActorRef): Actor.Receive = {
    case Terminated(receptionist) ⇒
      LOG.info("Lost contact with [{}], restablishing connection", receptionist)
      contacts foreach { _ ! Identify(None) }
      context.become(establishing)
    case _: ActorIdentity ⇒ // ok, from previous establish, already handled
    case msg => {
      LOG.info(s"Get msg ${msg.getClass.getSimpleName}, forwarding to ${receptionist.path}")
      receptionist forward msg
    }
  }
}

object MasterProxy {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[MasterProxy])
}