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

import akka.actor.{Actor, ExtendedActorSystem}
import org.apache.gearpump.serializer.FastKryoSerializer
import org.apache.gearpump.transport.netty.TaskMessage
import org.apache.gearpump.transport.{Express}
import org.slf4j.{LoggerFactory, Logger}

trait ExpressTransport {
  this: TaskActor =>

  final val express = Express(context.system)
  final val sendLater = context.actorSelection("../sendlater")
  final val system = context.system.asInstanceOf[ExtendedActorSystem]
  final val serializer = new FastKryoSerializer(system)
  final def local = express.localHost

  def transport(msg : AnyRef, remotes : TaskId *) = {

    var serializedMessage : Array[Byte] = null

    remotes.foreach { remote =>
      val transportId = TaskId.toLong(remote)
      val localActor = express.lookupLocalActor(transportId)
      if (localActor.isDefined) {
        //local
        localActor.get.tell(msg, Actor.noSender)
      } else {
      //remote
        if (null == serializedMessage) {
          serializedMessage = serializer.serialize(msg)
        }
        val taskMessage = new TaskMessage(transportId, serializedMessage)

        val remoteAddress = express.lookupRemoteAddress(transportId)
        if (remoteAddress.isDefined) {
          express.transport(taskMessage, remoteAddress.get)
        } else {
          sendLater ! taskMessage
        }
      }
    }
  }
}