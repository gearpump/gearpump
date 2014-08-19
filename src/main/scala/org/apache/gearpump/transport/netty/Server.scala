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

package org.apache.gearpump.transport.netty

import java.util.{List, Map}

import akka.actor.{ExtendedActorSystem, Actor, ActorRef}
import akka.remote.WireFormats.SerializedMessage
import akka.serialization.SerializationExtension
import com.google.protobuf.ByteString
import org.apache.gearpump.serializer.{FastKryoSerializer}
import org.apache.gearpump.transport.ActorLookupById
import org.jboss.netty.channel._
import org.jboss.netty.channel.group.{ChannelGroup, DefaultChannelGroup}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions._
import scala.concurrent.future

class Server(name: String, conf: NettyConfig, lookupActor : ActorLookupById) extends Actor {

  import org.apache.gearpump.transport.netty.Server._

  val allChannels: ChannelGroup = new DefaultChannelGroup("storm-server")

  val system = context.system.asInstanceOf[ExtendedActorSystem]

  val serializer = new FastKryoSerializer(system)


  def receive = msgHandler orElse channelManager

  def channelManager : Receive = {
    case AddChannel(channel) => allChannels.add(channel)
    case CloseChannel(channel) => {
      import context.dispatcher
      future {
        channel.close.awaitUninterruptibly
        allChannels.remove(channel)
      }
    }
  }

  def msgHandler : Receive = {
    case MsgBatch(msgs) => {
      msgs.groupBy(_.task()).map { taskBatch =>
        val (taskId, taskMessages) = taskBatch
        val actor = lookupActor.lookupActor(taskId)

        if (actor.isEmpty) {
          LOG.error(s"Cannot find actor for id: $taskId...")
        } else taskMessages.foreach { taskMessage =>
          val msg = serializer.deserialize(taskMessage.message())
          actor.get.tell(msg, Actor.noSender)
        }
      }
    }
  }

  override def postStop = {
    allChannels.close.awaitUninterruptibly
  }
}

object Server {
  private[netty] final val LOG: Logger = LoggerFactory.getLogger(classOf[Server])

  class ServerPipelineFactory(server: ActorRef) extends ChannelPipelineFactory {
    def getPipeline: ChannelPipeline = {
      val pipeline: ChannelPipeline = Channels.pipeline
      pipeline.addLast("decoder", new MessageDecoder)
      pipeline.addLast("encoder", new MessageEncoder)
      pipeline.addLast("handler", new ServerHandler(server))
      return pipeline
    }
  }

  class ServerHandler(server: ActorRef) extends SimpleChannelUpstreamHandler {

    override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      server ! AddChannel(e.getChannel)
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      val msgs: List[TaskMessage] = e.getMessage.asInstanceOf[List[TaskMessage]]
      if (msgs != null) {
        server ! MsgBatch(msgs)
      }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      LOG.error("server errors in handling the request", e.getCause)
      server ! CloseChannel(e.getChannel)
    }
  }

  case class AddChannel(channel: Channel)

  case class CloseChannel(channel: Channel)

  case class MsgBatch(messages: Iterable[TaskMessage])

}