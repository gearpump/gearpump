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

package org.apache.gearpump.streaming

import java.util

import akka.actor.Actor
import akka.pattern.pipe
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.transport.Express
import org.apache.gearpump.transport.netty.TaskMessage
import org.slf4j.{Logger, LoggerFactory}

case object TaskLocationReady

//TODO:
// 1. when any task dead in app master, appmaster should forward this message to here so that
//we will not send message to wrong address
//
class SendLater extends Actor {
  import context.dispatcher
  import org.apache.gearpump.streaming.SendLater.LOG

  private[this] val queue : util.ArrayDeque[TaskMessage] = new util.ArrayDeque[TaskMessage](SendLater.INITIAL_WINDOW_SIZE)

  private var taskLocationReady = false

  val express = Express(context.system)

  def receive : Receive = {
    case TaskLocations(locations) => {
      val result = locations.flatMap { kv =>
        val (host, set) = kv
        set.map(taskId => (TaskId.toLong(taskId), host))
      }
      express.remoteAddressMap.send(result)
      express.remoteAddressMap.future().map(_ => TaskLocationReady).pipeTo(self)
    }
    case msg : TaskMessage => {
      queue.add(msg)
      if (taskLocationReady) {
        sendPendingMessages()
      }
    }
    case TaskLocationReady => {
      taskLocationReady = true
      sendPendingMessages()
    }
  }

  def sendPendingMessages() : Unit = {
    var done = false
    while (!done) {
      val msg = queue.poll()
      if (msg != null) {
        val host = express.remoteAddressMap.get().get(msg.task())
        if (host.isDefined) {
          express.transport(msg, host.get)
        } else {
          LOG.error(s"Cannot find taskId ${TaskId.fromLong(msg.task())} in remoteAddressMap, dropping it...")
        }
      } else {
        done = true
      }
    }
  }
}

object SendLater {
  val INITIAL_WINDOW_SIZE = 1024 * 16
  val LOG: Logger = LoggerFactory.getLogger(classOf[SendLater])
 }