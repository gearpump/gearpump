/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package io.gearpump.util

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask

/** A helper util to send a message to remote actor and notify callback when timeout */
trait TimeOutScheduler {
  this: Actor =>
  import context.dispatcher

  def sendMsgWithTimeOutCallBack(
      target: ActorRef, msg: AnyRef, milliSeconds: Long, timeOutHandler: => Unit): Unit = {
    val result = target.ask(msg)(FiniteDuration(milliSeconds, TimeUnit.MILLISECONDS))
    result onSuccess {
      case msg =>
        self ! msg
    }
    result onFailure {
      case _ => timeOutHandler
    }
  }
}
