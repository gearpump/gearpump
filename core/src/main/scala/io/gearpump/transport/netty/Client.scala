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

package io.gearpump.transport.netty

import java.net.{ConnectException, InetSocketAddress}
import java.nio.channels.ClosedChannelException
import java.util
import java.util.Random
import java.util.concurrent.TimeUnit

import akka.actor.Actor
import io.gearpump.transport.HostPort
import io.gearpump.util.LogUtil
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import org.slf4j.Logger

import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

class Client(conf: NettyConfig, factory: ChannelFactory, hostPort : HostPort) extends Actor {
  import io.gearpump.transport.netty.Client._

  val name = s"netty-client-$hostPort"

  private final var bootstrap: ClientBootstrap = null
  private final var random: Random = new Random
  private var channel : Channel = null

  var batch = new util.ArrayList[TaskMessage]

  private val init = {
    bootstrap = NettyUtil.createClientBootStrap(factory, new ClientPipelineFactory(name), conf.buffer_size)
    self ! Connect(0)
  }

  def receive = messageHandler orElse connectionHandler

  def messageHandler : Receive = {
    case msg: TaskMessage =>
      batch.add(msg)
    case flush @ Flush(flushChannel)  =>
      if (channel != flushChannel) {
        Unit //Drop, as it belong to old channel flush message
      } else if (batch.size > 0 && flushChannel.isWritable) {
        send(flushChannel, batch.iterator)
        batch.clear()
        self ! flush
      } else {
        import context.dispatcher
        context.system.scheduler.scheduleOnce(new FiniteDuration(conf.flushCheckInterval, TimeUnit.MILLISECONDS), self, flush)
      }
  }

  def connectionHandler : Receive = {
    case ChannelReady(channel) =>
      this.channel = channel
      self ! Flush(channel)
    case Connect(tries) =>
      if (null == channel) {
        connect(tries)
      } else {
        LOG.error("there already exist a channel, will not establish a new one...")
      }
    case CompareAndReconnectIfEqual(oldChannel) =>
      if (oldChannel == channel) {
        channel = null
        self ! Connect(0)
      }
    case Close =>
      close()
      context.become(closed)
  }

  def closed : Receive = {
    case msg : AnyRef =>
      LOG.error(s"This client $name is closed, drop any message ${msg.getClass.getSimpleName}...")
  }

  private def connect(tries: Int) : Unit = {
    LOG.info(s"netty client try to connect to $name, tries: $tries")
    if (tries <= conf.max_retries) {
      val remote_addr = new InetSocketAddress(hostPort.host, hostPort.port)
      val future = bootstrap.connect(remote_addr)
      future success { current =>
        LOG.info(s"netty client successfully connectted to $name, tries: $tries")
        self ! ChannelReady(current)
      } fail { (current, ex) =>
        LOG.error(s"failed to connect to $name, reason: ${ex.getMessage}, class: ${ex.getClass}")
        current.close()
        import context.dispatcher
        context.system.scheduler.scheduleOnce(new FiniteDuration(getSleepTimeMs(tries), TimeUnit.MILLISECONDS), self, Connect(tries + 1))
      }
    } else {
      LOG.error(s"fail to connect to a remote host $name after retied $tries ...")
      self ! Close
    }
  }

  private def send(flushChannel: Channel, msgs: util.Iterator[TaskMessage]) {
    var messageBatch: MessageBatch = null

    while (msgs.hasNext) {
      val message: TaskMessage = msgs.next()
      if (null == messageBatch) {
        messageBatch = new MessageBatch(conf.messageBatchSize)
      }
      messageBatch.add(message)
      if (messageBatch.isFull) {
        val toBeFlushed: MessageBatch = messageBatch
        flushRequest(flushChannel, toBeFlushed)
        messageBatch = null
      }
    }
    if (null != messageBatch && !messageBatch.isEmpty) {
      flushRequest(flushChannel, messageBatch)
    }
  }

  private def close() {
    LOG.info(s"closing netty client $name...")
    if (null != channel) {
      channel.close()
      channel = null
    }
    batch = null
  }

  override def postStop() = {
    close()
  }

  private def flushRequest(channel: Channel, requests: MessageBatch) {
    val future: ChannelFuture = channel.write(requests)
    future.fail { (channel, ex) =>
      if (channel.isOpen) {
        channel.close
      }
      LOG.error(s"failed to send requests to ${channel.getRemoteAddress} ${ex.getClass.getSimpleName}")
      if (!ex.isInstanceOf[ClosedChannelException]) {
          LOG.error(ex.getMessage, ex)
      }
      self ! CompareAndReconnectIfEqual(channel)
    }
  }

  private def getSleepTimeMs(retries: Int): Long = {
    if (retries > 30) {
      conf.max_sleep_ms
    } else {
      val backoff = 1 << retries
      val sleepMs = conf.base_sleep_ms * Math.max(1, random.nextInt(backoff))
      if (sleepMs < conf.max_sleep_ms) sleepMs else conf.max_sleep_ms
    }
  }

  private def isChannelWritable = (null != channel) && channel.isWritable
}


object Client {
  val LOG: Logger = LogUtil.getLogger(getClass)

  //Reconnect if current channel equals channel
  case class CompareAndReconnectIfEqual(channel: Channel)

  case class Connect(tries: Int)
  case class ChannelReady(chanel : Channel)
  case object Close

  case class Flush(channel : Channel)

  class ClientErrorHandler(name: String) extends SimpleChannelUpstreamHandler {

    override def exceptionCaught(ctx: ChannelHandlerContext, event: ExceptionEvent) {
      event.getCause match {
        case ex: ConnectException => Unit
        case ex: ClosedChannelException =>
          LOG.warn("exception found when trying to close netty connection", ex.getMessage)
        case ex => LOG.error("Connection failed " + name, ex)
      }
    }
  }

  class ClientPipelineFactory(name: String) extends ChannelPipelineFactory {
    def getPipeline: ChannelPipeline = {
      val pipeline: ChannelPipeline = Channels.pipeline
      pipeline.addLast("decoder", new MessageDecoder)
      pipeline.addLast("encoder", new MessageEncoder)
      pipeline.addLast("handler", new ClientErrorHandler(name))
      pipeline
    }
  }

  implicit def channelFutureToChannelFutureOps(channel: ChannelFuture): ChannelFutureOps = new ChannelFutureOps(channel)

  class ChannelFutureOps(channelFuture: ChannelFuture) {

    def success(handler: (Channel => Unit)): ChannelFuture = {
      channelFuture.addListener(new ChannelFutureListener {
        def operationComplete(future: ChannelFuture) {
          if (future.isSuccess) {
            handler(future.getChannel)
          }
        }
      })
      channelFuture
    }

    def fail(handler: ((Channel, Throwable) => Unit)): ChannelFuture = {
      channelFuture.addListener(new ChannelFutureListener {
        def operationComplete(future: ChannelFuture) {
          if (!future.isSuccess) {
            handler(future.getChannel, future.getCause)
          }
        }
      })
      channelFuture
    }
  }
}
