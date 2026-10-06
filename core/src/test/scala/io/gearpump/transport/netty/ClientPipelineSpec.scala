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

import io.gearpump.cluster.TestUtil
import io.gearpump.testkit.MockitoSugar
import io.gearpump.transport.HostPort
import java.io.DataOutputStream
import java.net.{InetAddress, ServerSocket}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestProbe
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel.{AbstractChannelSink, Channel, ChannelEvent, ChannelPipeline, Channels}
import org.mockito.Mockito.{verify, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class ClientPipelineSpec
  extends AnyFlatSpec with Matchers with MockitoSugar with BeforeAndAfterAll {
  private val system = ActorSystem("client-pipeline", TestUtil.DEFAULT_CONFIG)
  private val conf = new NettyConfig(TestUtil.DEFAULT_CONFIG)

  private def header(length: Int): ChannelBuffer = {
    val buffer = ChannelBuffers.dynamicBuffer()
    buffer.writeInt(1)
    buffer.writeLong(2L)
    buffer.writeLong(3L)
    buffer.writeInt(length)
    buffer
  }

  it should "close malformed client frames and notify the owning client to reconnect" in {
    Seq(-1, MessageDecoder.MAX_FRAME_LENGTH + 1).foreach { length =>
      val client = TestProbe()(system)
      val pipeline = new Client.ClientPipelineFactory("test", conf, client.ref).getPipeline
      val channel = mock[Channel]
      when(channel.getPipeline).thenReturn(pipeline)
      pipeline.attach(channel, new AbstractChannelSink {
        override def eventSunk(pipeline: ChannelPipeline, event: ChannelEvent): Unit = ()
      })
      Channels.fireMessageReceived(channel, header(length))
      verify(channel).close()
      client.expectMsg(Client.CompareAndReconnectIfEqual(channel))
    }
  }

  it should "establish a fresh TCP connection after a peer sends a malformed frame" in {
    val listener = new ServerSocket(0, 2, InetAddress.getLoopbackAddress)
    listener.setSoTimeout(10000)
    val context = new Context(system, TestUtil.DEFAULT_CONFIG)
    import system.dispatcher
    val peer = Future {
      val first = listener.accept()
      try {
        first.setSoTimeout(10000)
        val output = new DataOutputStream(first.getOutputStream)
        output.writeInt(1)
        output.writeLong(2L)
        output.writeLong(3L)
        output.writeInt(MessageDecoder.MAX_FRAME_LENGTH + 1)
        output.flush()
        first.getInputStream.read() shouldBe -1
      } finally {
        first.close()
      }
      val second = listener.accept()
      second.close()
    }
    try {
      context.connect(HostPort(listener.getInetAddress.getHostAddress, listener.getLocalPort))
      Await.result(peer, 15.seconds)
    } finally {
      context.close()
      listener.close()
    }
  }

  override protected def afterAll(): Unit = {
    Await.result(system.terminate(), 15.seconds)
    super.afterAll()
  }
}
