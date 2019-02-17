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

import akka.actor.{Actor, ActorContext, ActorRef, ExtendedActorSystem}
import io.gearpump.transport.ActorLookupById
import io.gearpump.util.{AkkaHelper, LogUtil}
import java.util
import org.jboss.netty.channel._
import org.jboss.netty.channel.group.{ChannelGroup, DefaultChannelGroup}
import org.slf4j.Logger
import scala.collection.JavaConverters._
import scala.collection.immutable.IntMap
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
    case MsgBatch(msgs) =>
      msgs.asScala.groupBy(_.targetTask()).foreach { taskBatch =>
        val (taskId, taskMessages) = taskBatch
        val actor = lookupActor.lookupLocalActor(taskId)

        if (actor.isEmpty) {
          LOG.error(s"Cannot find actor for id: $taskId...")
        } else taskMessages.foreach { taskMessage =>
          actor.get.tell(taskMessage.message(),
            taskIdActorRefTranslation.translateToActorRef(taskMessage.sessionId()))
        }
      }
  }

  override def postStop(): Unit = {
    allChannels.close.awaitUninterruptibly
  }
}

object Server {

  class ServerPipelineFactory(server: ActorRef, conf: NettyConfig) extends ChannelPipelineFactory {
    def getPipeline: ChannelPipeline = {
      val pipeline: ChannelPipeline = Channels.pipeline
      pipeline.addLast("decoder", new MessageDecoder(conf.newTransportSerializer))
      pipeline.addLast("encoder", new MessageEncoder)
      pipeline.addLast("handler", new ServerHandler(server))
      pipeline
    }
  }

  class ServerHandler(server: ActorRef) extends SimpleChannelUpstreamHandler {
    private[netty] final val LOG: Logger = LogUtil.getLogger(getClass, context = server.path.name)

    override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      server ! AddChannel(e.getChannel)
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      val msgs: util.List[TaskMessage] = e.getMessage.asInstanceOf[util.List[TaskMessage]]
      if (msgs != null) {
        server ! MsgBatch(msgs)
      }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      LOG.error("server errors in handling the request", e.getCause)
      server ! CloseChannel(e.getChannel)
    }
  }

  class TaskIdActorRefTranslation(context: ActorContext) {
    private var taskIdtoActorRef = IntMap.empty[ActorRef]

    /** 1-1 mapping from session id to fake ActorRef */
    def translateToActorRef(sessionId: Int): ActorRef = {
      if (!taskIdtoActorRef.contains(sessionId)) {

        // A fake ActorRef for performance optimization.
        val actorRef = AkkaHelper.actorFor(context.system, s"/session#$sessionId")
        taskIdtoActorRef += sessionId -> actorRef
      }
      taskIdtoActorRef.get(sessionId).get
    }
  }

  case class AddChannel(channel: Channel)

  case class CloseChannel(channel: Channel)

  case class MsgBatch(messages: java.lang.Iterable[TaskMessage])

}