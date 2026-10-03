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

package io.gearpump.transport.netty

import org.apache.pekko.actor.{Actor, ActorContext, ActorRef, ExtendedActorSystem}
import io.gearpump.transport.ActorLookupById
import io.gearpump.util.{PekkoHelper, LogUtil}
import java.util
import org.jboss.netty.channel._
import org.jboss.netty.channel.group.{ChannelGroup, DefaultChannelGroup}
import org.slf4j.Logger
import scala.jdk.CollectionConverters._
import scala.concurrent.Future

/** Netty server actor, message received will be forward to the target on the address line. */
class Server(name: String, lookupActor: ActorLookupById)
  extends Actor {

  private[netty] final val LOG: Logger = LogUtil.getLogger(getClass, context = name)
  import io.gearpump.transport.netty.Server._

  val allChannels: ChannelGroup = new DefaultChannelGroup("gearpump-server")

  val system = context.system.asInstanceOf[ExtendedActorSystem]

  def receive: Receive = msgHandler orElse channelManager
  // As we will only transfer TaskId on the wire,
  // this object will translate taskId to or from ActorRef
  private val taskIdActorRefTranslation = new TaskIdActorRefTranslation(context)

  def channelManager: Receive = {
    case AddChannel(channel) => allChannels.add(channel)
    case CloseChannel(channel) =>
      import context.dispatcher
      Future {
        channel.close.awaitUninterruptibly
        allChannels.remove(channel)
      }
  }

  def msgHandler: Receive = {
    case MsgBatch(msgs, capability) if io.gearpump.security.ControlCapability.matches(
        io.gearpump.security.ControlCapability.token(system.settings.config,
          io.gearpump.security.ControlCapability.AppKey), capability) =>
      msgs.asScala.groupBy(_.targetTask()).foreach { taskBatch =>
        val (taskId, taskMessages) = taskBatch
        val actor = lookupActor.lookupLocalActor(taskId)

        if (actor.isEmpty) {
          LOG.error(s"Cannot find actor for id: $taskId...")
        } else taskMessages.foreach { taskMessage =>
          actor.get.tell(taskMessage.message(),
            taskIdActorRefTranslation.translateToActorRef(taskMessage.sessionId(), taskMessage.sourceTask()))
        }
      }
    case _: MsgBatch => // A remote actor cannot bypass transport authentication.
  }

  override def postStop(): Unit = {
    allChannels.close.awaitUninterruptibly
  }
}

object Server {

  class ServerPipelineFactory(server: ActorRef, conf: NettyConfig) extends ChannelPipelineFactory {
    def getPipeline: ChannelPipeline = {
      val pipeline: ChannelPipeline = Channels.pipeline
      val engine = io.gearpump.security.ClusterTls.serverEngine(conf.tls)
      // Netty 3 SSLEngine compatibility; TLS 1.2 remains authenticated AEAD encryption.
      engine.setEnabledProtocols(Array("TLSv1.2"))
      pipeline.addLast("tls", new org.jboss.netty.handler.ssl.SslHandler(engine))
      pipeline.addLast("application-auth", new AuthenticatedFrames.Decoder(conf.applicationCapability))
      pipeline.addLast("auth-encoder", new AuthenticatedFrames.Encoder(conf.applicationCapability))
      pipeline.addLast("decoder", new MessageDecoder(conf.newTransportSerializer))
      pipeline.addLast("encoder", new MessageEncoder)
      pipeline.addLast("handler", new ServerHandler(server, conf.applicationCapability))
      pipeline
    }
  }

  class ServerHandler(server: ActorRef, capability: String) extends SimpleChannelUpstreamHandler {
    private[netty] final val LOG: Logger = LogUtil.getLogger(getClass, context = server.path.name)

    override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
      server ! AddChannel(e.getChannel)
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
      val msgs: util.List[TaskMessage] = e.getMessage.asInstanceOf[util.List[TaskMessage]]
      if (msgs != null) {
        server ! MsgBatch(msgs, capability)
      }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent): Unit = {
      LOG.error("server errors in handling the request", e.getCause)
      e.getChannel.close()
      server ! CloseChannel(e.getChannel)
    }
  }

  class TaskIdActorRefTranslation(context: ActorContext) {
    private var taskIdtoActorRef = Map.empty[(Int, Long), ActorRef]

    /** 1-1 mapping from session id to fake ActorRef */
    def translateToActorRef(sessionId: Int, sourceTask: Long): ActorRef = {
      val key = (sessionId, sourceTask)
      if (!taskIdtoActorRef.contains(key)) {

        // A fake ActorRef for performance optimization.
        val actorRef = PekkoHelper.sessionActorFor(context.system, sessionId, sourceTask)
        // Bound the performance cache; an authenticated stream cannot grow it forever.
        if (taskIdtoActorRef.size >= 65536) taskIdtoActorRef = Map.empty
        taskIdtoActorRef += key -> actorRef
      }
      taskIdtoActorRef.get(key).get
    }
  }

  case class AddChannel(channel: Channel)

  case class CloseChannel(channel: Channel)

  case class MsgBatch(messages: java.lang.Iterable[TaskMessage], capability: String = "") {
    override def toString: String = "MsgBatch(<redacted>)"
  }

}
