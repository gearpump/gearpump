package org.apache.gearpump
/**
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

import java.util.concurrent.TimeUnit

import akka.actor.Actor.Receive
import akka.actor._
import org.apache.gearpump.service.SimpleKVService
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object ActorUtil {
  private val LOG: Logger = LoggerFactory.getLogger(ActorUtil.getClass)

  def getFullPath(context: ActorContext): String = {
    context.self.path.toStringWithAddress(
      context.system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress)
  }

  def getMaster(system : ActorSystem) : ActorRef = {

    def tryGetMaster(attempt : Int, maxAttempt : Int, interval : Int) : Future[ActorRef] = {
      LOG.info("trying to look up master, attempt: " + attempt);

      if (attempt > maxAttempt) {
        Future.failed(new Exception("max attempt passed"))
      } else {
        val masterURL = SimpleKVService.get("master")
        LOG.info("masterURL: " + masterURL);
        if (masterURL == "" || masterURL == null) {
          Thread.sleep(interval * 1000); // sleep 3s
          tryGetMaster(attempt + 1, maxAttempt, interval)
        } else {
          val master = system.actorSelection(masterURL);
          master.resolveOne(Duration(1, TimeUnit.SECONDS)).recoverWith {
            case ActorNotFound(_) =>
              Thread.sleep(interval * 1000); //sleep 3s
              tryGetMaster(attempt + 1, maxAttempt, interval)
          }
        }
      }
    }

    val master = Await.result(tryGetMaster(0, 10, 3), Duration(1, TimeUnit.MINUTES)).asInstanceOf[ActorRef]
    master
  }

  def defaultMsgHandler : Receive = {
    case msg : Any =>
      LOG.error("Cannot find a matching message, " + msg.getClass.toString)
  }
}
