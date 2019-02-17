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

import java.net.InetSocketAddress
import java.util.concurrent.{Executors, ThreadFactory}
import org.jboss.netty.bootstrap.{ClientBootstrap, ServerBootstrap}
import org.jboss.netty.channel.{Channel, ChannelFactory, ChannelPipelineFactory}
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory

object NettyUtil {

  def newNettyServer(
      name: String,
      pipelineFactory: ChannelPipelineFactory,
      buffer_size: Int,
      inputPort: Int = 0): (Int, Channel) = {
    val bossFactory: ThreadFactory = new NettyRenameThreadFactory(name + "-boss")
    val workerFactory: ThreadFactory = new NettyRenameThreadFactory(name + "-worker")
    val factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(bossFactory),
      Executors.newCachedThreadPool(workerFactory), 1)

    val bootstrap = createServerBootStrap(factory, pipelineFactory, buffer_size)
    val channel: Channel = bootstrap.bind(new InetSocketAddress(inputPort))
    val port = channel.getLocalAddress().asInstanceOf[InetSocketAddress].getPort()
    (port, channel)
  }

  def createServerBootStrap(
      factory: ChannelFactory, pipelineFactory: ChannelPipelineFactory, buffer_size: Int)
    : ServerBootstrap = {
    val bootstrap = new ServerBootstrap(factory)
    bootstrap.setOption("child.tcpNoDelay", true)
    bootstrap.setOption("child.receiveBufferSize", buffer_size)
    bootstrap.setOption("child.keepAlive", true)
    bootstrap.setPipelineFactory(pipelineFactory)
    bootstrap
  }

  def createClientBootStrap(
      factory: ChannelFactory, pipelineFactory: ChannelPipelineFactory, buffer_size: Int)
    : ClientBootstrap = {
    val bootstrap = new ClientBootstrap(factory)
    bootstrap.setOption("tcpNoDelay", true)
    bootstrap.setOption("sendBufferSize", buffer_size)
    bootstrap.setOption("keepAlive", true)
    bootstrap.setPipelineFactory(pipelineFactory)
    bootstrap
  }
}
