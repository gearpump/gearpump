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

package org.apache.gearpump.streaming.task

import akka.actor.{ActorRef, ExtendedActorSystem}
import org.apache.gearpump.serializer.{SerializerPool, KryoPool, FastKryoSerializer}
import org.apache.gearpump.transport.netty.TaskMessage
import org.apache.gearpump.transport.{Express, HostPort}

import scala.collection.mutable

trait ExpressTransport {
  this: TaskActor =>

  final val express = Express(context.system)
  implicit val system = context.system.asInstanceOf[ExtendedActorSystem]

  final def local = express.localHost
  lazy val sourceId = TaskId.toLong(taskId)

  lazy val sessionRef: ActorRef = {
    system.actorFor(s"/session#$sessionId")
  }

  lazy val sendLater = new SendLater(express, serializerPool, sessionRef)

  def transport(msg : AnyRef, remotes : TaskId *): Unit = {

    var serializedMessage : Array[Byte] = null

    remotes.foreach { remote =>
      val transportId = TaskId.toLong(remote)
      val localActor = express.lookupLocalActor(transportId)
      if (localActor.isDefined) {
        //local
        if(sendLater.hasPendingMessages){
          sendLater.flushPendingMessages(localActor.get, transportId)
        }
        localActor.get.tell(msg, sessionRef)
      } else {
      //remote
        if (null == serializedMessage) {
          serializedMessage = serializerPool.get(Thread.currentThread().getId).serialize(msg)
        }
        val taskMessage = new TaskMessage(sessionId, transportId, sourceId, serializedMessage)

        val remoteAddress = express.lookupRemoteAddress(transportId)
        if (remoteAddress.isDefined) {
          if(sendLater.hasPendingMessages){
            sendLater.flushPendingMessages(remoteAddress.get, transportId)
          }
          express.transport(taskMessage, remoteAddress.get)
        } else {
          sendLater.addMessage(transportId, taskMessage)
        }
      }
    }
  }
}

class SendLater(express: Express, serializerPool: SerializerPool, sender: ActorRef){
  private var buffer = Map.empty[Long, mutable.Queue[TaskMessage]]

  def addMessage(transportId: Long, taskMessage: TaskMessage) = {
    val queue = buffer.getOrElse(transportId, mutable.Queue.empty[TaskMessage])
    queue.enqueue(taskMessage)
    buffer += transportId -> queue
  }

  private def sendPendingMessages(transportId: Long) = {
    val localActor = express.lookupLocalActor(transportId)
    if (localActor.isDefined) {
      flushPendingMessages(localActor.get, transportId)
    } else {
      val remoteAddress = express.lookupRemoteAddress(transportId)
      flushPendingMessages(remoteAddress.get, transportId)
    }
  }

  def flushPendingMessages(localActor: ActorRef, transportId: Long) = {
    val queue = buffer.getOrElse(transportId, mutable.Queue.empty[TaskMessage])
    while (queue.nonEmpty) {
      val taskMessage = queue.dequeue()
      val msg = serializerPool.get(Thread.currentThread().getId).deserialize(taskMessage.message())
      localActor.tell(msg, sender)
    }
  }

  def flushPendingMessages(remoteAddress: HostPort, transportId: Long) = {
    val queue = buffer.getOrElse(transportId, mutable.Queue.empty[TaskMessage])
    while (queue.nonEmpty) {
      val taskMessage = queue.dequeue()
      express.transport(taskMessage, remoteAddress)
    }
  }

  def sendAllPendingMsgs(): Unit = {
    buffer.keySet.foreach(sendPendingMessages)
    buffer = Map.empty[Long, mutable.Queue[TaskMessage]]
  }

  def hasPendingMessages: Boolean = buffer.nonEmpty
}