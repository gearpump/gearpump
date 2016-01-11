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

import java.io.Closeable
import java.util.concurrent._

import akka.actor.{ActorRef, ActorSystem, Props}
import com.typesafe.config.Config
import io.gearpump.transport.netty.Server.ServerChannelInitiallizer
import io.gearpump.transport.{ActorLookupById, HostPort}
import io.gearpump.util.{Constants, LogUtil}
import io.netty.channel.nio.NioEventLoopGroup
import org.slf4j.Logger

import scala.collection.JavaConversions._
import scala.language.implicitConversions

object Context {
  private final val LOG: Logger = LogUtil.getLogger(getClass)

  implicit def toCloseable(fun : () => Any): Closeable = new Closeable {
    override def close = fun()
  }
}

class Context(system : ActorSystem, conf: NettyConfig) extends IContext {
import io.gearpump.transport.netty.Context._

  def this(system : ActorSystem, conf : Config) {
    this(system, new NettyConfig(conf))
  }

  private val closeHandler = new ConcurrentLinkedQueue[Closeable]()
  private val nettyDispatcher = system.settings.config.getString(Constants.NETTY_DISPATCHER)
  val maxWorkers: Int = 1

  private val workerEventLoopGroup = NettyUtil.createNioEventLoopGroup(maxWorkers, "client-worker")

  def bind(name: String, lookupActor : ActorLookupById, deserializeFlag : Boolean = true, inputPort: Int = 0): Int = {
    //TODO: whether we should expose it as application config?
    val server = system.actorOf(Props(classOf[Server], name, conf, lookupActor, deserializeFlag).withDispatcher(nettyDispatcher), name)
    val bossEventLoopGroup = NettyUtil.createNioEventLoopGroup(0, name + "-boss")
    val workerEventLoopGroup = NettyUtil.createNioEventLoopGroup(1, name + "-worker")
    val channelInitializer = new ServerChannelInitiallizer(server, conf)
    val (port, channel) = NettyUtil.newNettyServer(bossEventLoopGroup, workerEventLoopGroup, channelInitializer, 5242880, inputPort)
    closeHandler.add { () =>
      system.stop(server)
      channel.close()
      bossEventLoopGroup.shutdownGracefully()
      workerEventLoopGroup.shutdownGracefully()
      LOG.info("Closing all server resources....")
    }
    port
  }

  def connect(hostPort : HostPort) : ActorRef = {
    val client = system.actorOf(Props(classOf[Client], conf, workerEventLoopGroup, hostPort).withDispatcher(nettyDispatcher))
    closeHandler.add { () =>
      LOG.info("closing Client actor....")
      system.stop(client)
    }
    client
  }

  /**
   * terminate this context
   */
  def close {
    LOG.info(s"Context.term, cleanup resources...., we have ${closeHandler.size()} items to close...")
    // clean up resource in reverse order so that client actor can be cleaned
    // before clientChannelFactory
    closeHandler.iterator().toArray.reverse.foreach(_.close())
    workerEventLoopGroup.shutdownGracefully()
  }
}

