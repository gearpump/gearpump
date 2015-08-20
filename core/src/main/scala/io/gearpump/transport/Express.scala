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

package io.gearpump.transport

import akka.actor._
import akka.agent.Agent
import io.gearpump.transport.netty.Client.Close
import io.gearpump.transport.netty.{Context, TaskMessage}
import io.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.immutable.LongMap
import scala.concurrent._

trait ActorLookupById {
  def lookupLocalActor(id: Long): Option[ActorRef]
}

class Express(val system: ExtendedActorSystem) extends Extension with ActorLookupById {

  import io.gearpump.transport.Express._
  import system.dispatcher
  val localActorMap = Agent(LongMap.empty[ActorRef])
  val remoteAddressMap = Agent(Map.empty[Long, HostPort])

  val remoteClientMap = Agent(Map.empty[HostPort, ActorRef])

  val conf = system.settings.config

  lazy val (context, serverPort, localHost) = init

  lazy val init = {
    LOG.info(s"Start Express init ...${system.name}")
    val context = new Context(system, conf)
    val serverPort = context.bind("netty-server", this)
    val localHost = HostPort(system.provider.getDefaultAddress.host.get, serverPort)
    LOG.info(s"binding to netty server $localHost")

    system.registerOnTermination(new Runnable {
      override def run = context.close
    })
    (context, serverPort, localHost)
  }

  def unregisterLocalActor(id : Long) : Unit = {
    localActorMap.sendOff(_ - id)
  }

  def startClients(hostPorts: Set[HostPort]): Future[Map[HostPort, ActorRef]] = {
    val clientsToClose = remoteClientMap.get().filterKeys(!hostPorts.contains(_)).keySet
    closeClients(clientsToClose)
    hostPorts.toList.foldLeft(Future(Map.empty[HostPort, ActorRef])) { (future, hostPort) =>
      remoteClientMap.alter { map =>
        if (!map.contains(hostPort)) {
          val actor = context.connect(hostPort)
          map + (hostPort -> actor)
        } else {
          map
        }
      }
    }
  }

  def closeClients(hostPorts: Set[HostPort]): Future[Map[HostPort, ActorRef]] = {
    remoteClientMap.alter { map =>
      map.filterKeys(hostPorts.contains).foreach{ hostAndClient =>
        val (_, client) = hostAndClient
        client ! Close
      }
      map -- hostPorts
    }
  }

  def registerLocalActor(id : Long, actor: ActorRef): Unit = {
    LOG.info(s"RegisterLocalActor: $id, actor: ${actor.path.name}")
    init
    localActorMap.sendOff(_ + (id -> actor))
  }

  def lookupLocalActor(id: Long) = localActorMap.get().get(id)

  def lookupRemoteAddress(id : Long) = remoteAddressMap.get().get(id)

  //transport to remote address
  def transport(taskMessage: TaskMessage, remote: HostPort): Unit = {

    val remoteClient = remoteClientMap.get.get(remote)
    if (remoteClient.isDefined) {
      remoteClient.get.tell(taskMessage, Actor.noSender)
    } else {
      val errorMsg = s"Clients has not been launched properly before transporting messages, the destination is $remote"
      LOG.error(errorMsg)
      throw new Exception(errorMsg)
    }
  }
}

object Express extends ExtensionId[Express] with ExtensionIdProvider {
  val LOG: Logger = LogUtil.getLogger(getClass)

  override def get(system: ActorSystem): Express = super.get(system)

  override def lookup = Express

  override def createExtension(system: ExtendedActorSystem): Express = new Express(system)
}