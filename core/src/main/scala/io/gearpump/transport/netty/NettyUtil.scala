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

import java.net.InetSocketAddress
import java.util.concurrent.ThreadFactory

import io.gearpump.util.Util
import io.netty.bootstrap.{ServerBootstrap, Bootstrap}
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.{NioServerSocketChannel, NioSocketChannel}
import io.netty.channel._

object NettyUtil {

  def createNioEventLoopGroup(threadNum: Int, threadFactoryName: String): NioEventLoopGroup = {
    val factory = new NettyRenameThreadFactory(threadFactoryName)
    new NioEventLoopGroup(threadNum, factory)
  }

  def newNettyServer(
      bossEventLoopGroup: EventLoopGroup,
      workerEventLoopGroup: EventLoopGroup,
      channelInitializer: ChannelInitializer[Channel],
      buffer_size: Int,
      inputPort: Int): (Int, Channel) = {

    val port = if(inputPort == 0) Util.findFreePort.get else inputPort
    val bootstrap = new ServerBootstrap()
      .group(bossEventLoopGroup, workerEventLoopGroup)
      .channel(classOf[NioServerSocketChannel])
      .childOption(ChannelOption.TCP_NODELAY.asInstanceOf[ChannelOption[Boolean]], true)
      .childOption(ChannelOption.SO_RCVBUF.asInstanceOf[ChannelOption[Int]], buffer_size)
      .childOption(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Boolean]], true)
      .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      .childHandler(channelInitializer)

    val channel = bootstrap.bind(new InetSocketAddress(port)).sync().channel()
    (port, channel)
  }

  def createClientBootStrap(eventLoopGroup: EventLoopGroup, channelInitializer: ChannelInitializer[Channel], buffer_size: Int) = {
    val bootstrap = new Bootstrap().group(eventLoopGroup)
      .channel(classOf[NioSocketChannel])
      .option(ChannelOption.TCP_NODELAY.asInstanceOf[ChannelOption[Boolean]], true)
      .option(ChannelOption.SO_SNDBUF.asInstanceOf[ChannelOption[Int]], buffer_size)
      .option(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Boolean]], true)
      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      .handler(channelInitializer)
    bootstrap
  }
}
