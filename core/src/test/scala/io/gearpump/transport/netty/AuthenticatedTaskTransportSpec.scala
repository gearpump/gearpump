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

import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.testkit.TestProbe
import com.typesafe.config.ConfigValueFactory
import io.gearpump.cluster.TestUtil
import io.gearpump.security.{ClusterTls, ControlCapability}
import io.gearpump.transport.ActorLookupById
import io.gearpump.transport.MockTransportSerializer
import io.gearpump.transport.MockTransportSerializer.NettyMessage
import io.gearpump.util.Constants
import java.io.{DataInput, IOException}
import java.net.Socket
import java.util.concurrent.atomic.AtomicInteger
import javax.net.ssl.{KeyManager, SSLContext, TrustManagerFactory, SSLSocket}
import java.security.KeyStore
import org.jboss.netty.buffer.ChannelBuffer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.Await
import scala.concurrent.duration._

object CountingTaskSerializer { val reads = new AtomicInteger() }
class CountingTaskSerializer extends MockTransportSerializer {
  override def deserialize(in: DataInput, length: Int): AnyRef = {
    CountingTaskSerializer.reads.incrementAndGet()
    super.deserialize(in, length)
  }
}
class AuthenticatedTaskTransportSpec extends AnyFlatSpec with Matchers {
  it should "reject plaintext, anonymous TLS and wrong-app frames before deserialization" in {
    val capability = ControlCapability.random()
    val config = ControlCapability.runtimeConfig(TestUtil.DEFAULT_CONFIG, 1, capability)
      .withValue(Constants.GEARPUMP_TRANSPORT_SERIALIZER,
        ConfigValueFactory.fromAnyRef(classOf[CountingTaskSerializer].getName))
    implicit val system = ActorSystem("task-auth-wire", config)
    val server = TestProbe()
    val (port, channel) = NettyUtil.newNettyServer("task-auth-wire",
      new Server.ServerPipelineFactory(server.ref, new NettyConfig(config)), 1024)
    def socket(context: SSLContext): SSLSocket = {
      val socket = context.getSocketFactory.createSocket("127.0.0.1", port).asInstanceOf[SSLSocket]
      socket.setEnabledProtocols(Array("TLSv1.2"))
      socket.setSoTimeout(3000)
      socket
    }
    try {
      CountingTaskSerializer.reads.set(0)
      val plain = new Socket("127.0.0.1", port)
      plain.setSoTimeout(3000)
      try {
        plain.getOutputStream.write(new Array[Byte](24))
        val input = plain.getInputStream
        var count = 0
        var byte = input.read()
        while (byte >= 0 && count <= 256) { count += 1; byte = input.read() }
        byte shouldBe -1 // A fatal TLS alert may precede close.
        count should be <= 256
      } finally plain.close()

      val tls = config.getConfig("gearpump.security.tls")
      val store = KeyStore.getInstance(tls.getString("store-type"))
      val input = java.nio.file.Files.newInputStream(java.nio.file.Paths.get(tls.getString("trust-store")))
      try store.load(input, tls.getString("password").toCharArray) finally input.close()
      val trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      trust.init(store)
      val anonymous = SSLContext.getInstance("TLS")
      anonymous.init(Array.empty[KeyManager], trust.getTrustManagers, null)
      val noCertificate = socket(anonymous)
      try intercept[IOException] { noCertificate.startHandshake() } finally noCertificate.close()

      val wrongApp = socket(ClusterTls.context(config))
      try {
        wrongApp.startHandshake()
        val batch = new MessageBatch(1024, new CountingTaskSerializer)
        batch.add(new TaskMessage(9, 1, 2, NettyMessage(10)))
        val encoder = new AuthenticatedFrames.Encoder(ControlCapability.random()) {
          def frame(bytes: ChannelBuffer): ChannelBuffer = encode(null, null, bytes).asInstanceOf[ChannelBuffer]
        }
        val frame = encoder.frame(batch.buffer())
        val bytes = new Array[Byte](frame.readableBytes())
        frame.readBytes(bytes)
        wrongApp.getOutputStream.write(bytes)
        try wrongApp.getInputStream.read() shouldBe -1 catch {
          case _: java.net.SocketTimeoutException => fail("Wrong-app connection remained open")
          case _: IOException => ()
        }
      } finally wrongApp.close()
      server.receiveWhile(100.millis) { case message =>
        message should not be a[Server.MsgBatch]
      }
      CountingTaskSerializer.reads.get() shouldBe 0
      val records = TestProbe()
      val lookup = new ActorLookupById {
        override def lookupLocalActor(id: Long): Option[ActorRef] = Some(records.ref)
      }
      val actor = system.actorOf(Props(new Server("actor-bypass", lookup)))
      val injected = java.util.Arrays.asList(new TaskMessage(7, 1, 2, NettyMessage(99)))
      actor ! Server.MsgBatch(injected)
      actor ! Server.MsgBatch(injected, ControlCapability.random())
      records.expectNoMessage(100.millis)
      actor ! Server.MsgBatch(injected, capability)
      records.expectMsg(NettyMessage(99))
    } finally {
      channel.close().awaitUninterruptibly()
      channel.getFactory.releaseExternalResources()
      Await.result(system.terminate(), 10.seconds)
    }
  }
}
