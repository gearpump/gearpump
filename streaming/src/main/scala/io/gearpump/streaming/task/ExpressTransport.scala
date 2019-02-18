/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.streaming.task

import akka.actor.{ActorRef, ExtendedActorSystem}
import io.gearpump.Message
import io.gearpump.transport.{Express, HostPort}
import io.gearpump.transport.netty.TaskMessage
import io.gearpump.util.AkkaHelper
/**
 * ExpressTransport wire the networking function from default akka
 * networking to customized implementation [[io.gearpump.transport.Express]].
 *
 * See [[io.gearpump.transport.Express]] for more information.
 */
trait ExpressTransport {
  this: TaskActor =>

  final val express = Express(context.system)
  implicit val system = context.system.asInstanceOf[ExtendedActorSystem]

  final def local: HostPort = express.localHost
  lazy val sourceId = TaskId.toLong(taskId)

  lazy val sessionRef: ActorRef = {
    AkkaHelper.actorFor(system, s"/session#$sessionId")
  }

  def transport(msg: AnyRef, remotes: TaskId*): Unit = {
    var serializedMessage: AnyRef = null

    remotes.foreach { remote =>
      val transportId = TaskId.toLong(remote)
      val localActor = express.lookupLocalActor(transportId)
      if (localActor.isDefined) {
        localActor.get.tell(msg, sessionRef)
      } else {
        if (null == serializedMessage) {
          msg match {
            case message: Message =>
              val bytes = serializerPool.get().serialize(message.value)
              serializedMessage = SerializedMessage(message.timestamp.toEpochMilli, bytes)
            case _ => serializedMessage = msg
          }
        }
        val taskMessage = new TaskMessage(sessionId, transportId, sourceId, serializedMessage)

        val remoteAddress = express.lookupRemoteAddress(transportId)
        if (remoteAddress.isDefined) {
          express.transport(taskMessage, remoteAddress.get)
        } else {
          LOG.error(
            s"Can not find target task $remote, maybe the application is undergoing recovery")
        }
      }
    }
  }
}