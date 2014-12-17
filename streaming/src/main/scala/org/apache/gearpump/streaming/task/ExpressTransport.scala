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
import org.apache.gearpump.serializer.FastKryoSerializer
import org.apache.gearpump.transport.Express
import org.apache.gearpump.transport.netty.TaskMessage

import scala.collection.mutable

trait ExpressTransport {
  this: TaskActor =>

  final val express = Express(context.system)
  final val system = context.system.asInstanceOf[ExtendedActorSystem]
  final val serializer = new FastKryoSerializer(system)
  final def local = express.localHost
  lazy val sourceId = TaskId.toLong(this.taskId)

  val senderLater = new SendLater(express, serializer, self)

  def transport(msg : AnyRef, remotes : TaskId *) = {

    var serializedMessage : Array[Byte] = null

    remotes.foreach { remote =>
      val transportId = TaskId.toLong(remote)
      val localActor = express.lookupLocalActor(transportId)
      if (localActor.isDefined) {
        //local
        senderLater.sendToLocal(transportId)
        localActor.get.tell(msg, self)
      } else {
      //remote
        if (null == serializedMessage) {
          serializedMessage = serializer.serialize(msg)
        }
        val taskMessage = new TaskMessage(transportId, sourceId, serializedMessage)

        val remoteAddress = express.lookupRemoteAddress(transportId)
        if (remoteAddress.isDefined) {
          senderLater.sendToRemote(transportId)
          express.transport(taskMessage, remoteAddress.get)
        } else {
          senderLater.addMessage(transportId, taskMessage)
        }
      }
    }
  }

  def emptyBuffer: Unit = senderLater.emptyBuffer
}

class SendLater(express: Express, serializer: FastKryoSerializer, sender: ActorRef){
  private var buffer = Map.empty[Long, mutable.Queue[TaskMessage]]

  def addMessage(transportId: Long, taskMessage: TaskMessage) = {
    val queue = buffer.getOrElse(transportId, mutable.Queue.empty[TaskMessage])
    queue.enqueue(taskMessage)
    buffer += transportId -> queue
  }

  private def sendMsgInBuffer(transportId: Long) = {
    val localActor = express.lookupLocalActor(transportId)
    if (localActor.isDefined) {
      sendToLocal(transportId)
    } else {
      sendToRemote(transportId)
    }
  }

  def sendToLocal(transportId: Long) = {
    val queue = buffer.getOrElse(transportId, mutable.Queue.empty[TaskMessage])
    while (queue.nonEmpty) {
      val taskMessage = queue.dequeue()
      val localActor = express.lookupLocalActor(transportId)
      val msg = serializer.deserialize(taskMessage.message())
      localActor.get.tell(msg, sender)
    }
  }

  def sendToRemote(transportId: Long) = {
    val queue = buffer.getOrElse(transportId, mutable.Queue.empty[TaskMessage])
    while (queue.nonEmpty) {
      val taskMessage = queue.dequeue()
      val remoteAddress = express.lookupRemoteAddress(transportId)
      express.transport(taskMessage, remoteAddress.get)
    }
  }

  def emptyBuffer: Unit = {
    buffer.keySet.foreach(sendMsgInBuffer)
    buffer = Map.empty[Long, mutable.Queue[TaskMessage]]
  }
}