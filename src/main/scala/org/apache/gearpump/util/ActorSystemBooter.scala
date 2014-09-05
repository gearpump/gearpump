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

package org.apache.gearpump.util

import akka.actor._
import com.typesafe.config.Config
import org.apache.gearpump.cluster.Configs
import org.slf4j.{Logger, LoggerFactory}

/**
 * Start a actor system, and will send the system adress to report back actor
 * Example usage: main reportBackActorPath
 */

class ActorSystemBooter(config : Config) {
  import org.apache.gearpump.util.ActorSystemBooter._

  def boot(name : String, reportBackActor : String) : ActorSystem = {
    val system = ActorSystem(name, config)
    // daemon path: http://{system}@{ip}:{port}/daemon
    system.actorOf(Props(classOf[Daemon], name, reportBackActor), "daemon")
    LOG.info(s"Booting Actor System $name reporting back url: $reportBackActor")
    system
  }
}

object ActorSystemBooter  {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[ActorSystemBooter])

  def create(config : Config) : ActorSystemBooter = new ActorSystemBooter(config)

  def main (args: Array[String]) {
    val name = args(0)
    val reportBack = args(1)
    val config = Configs.SYSTEM_DEFAULT_CONFIG
    create(config).boot(name, reportBack).awaitTermination
  }

  case class BindLifeCycle(actor : ActorRef)
  case class CreateActor(props : Props, name : String)
  case class RegisterActorSystem(systemPath : String)

  class Daemon(name : String, reportBack : String) extends Actor {
    def receive : Receive = {
      //Bind life cycle with sender, if sender dies, the daemon dies, then the system dies
      case BindLifeCycle(actor) =>
        LOG.info(s"ActorSystem $name Binding life cycle with actor: $actor")
        context.watch(actor)
      // Send PoisonPill to the daemon to kill the actorsystem

      case CreateActor(props : Props, name : String) =>
        LOG.info(s"creating actor $name")
        context.actorOf(props, name)
      case PoisonPill =>
        context.stop(self)
      case Terminated(actor) =>
        LOG.info(s"System $name Watched actor is terminated $actor")
        context.stop(self)
    }

    override def postStop : Unit = {
      LOG.info(s"Actor System $name is shutting down...")
      context.system.shutdown()
    }

    override def preStart : Unit = {
      val reportBackActor = context.actorSelection(reportBack)
      reportBackActor ! RegisterActorSystem(ActorUtil.getSystemPath(context.system))
    }
  }
}