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

package io.gearpump.transport

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestProbe
import io.gearpump.cluster.TestUtil
import io.gearpump.transport.MockTransportSerializer.NettyMessage
import io.gearpump.transport.netty.{Context, TaskMessage}
import io.gearpump.util.Util
import java.util.concurrent.TimeUnit
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.mockito.MockitoSugar
import scala.concurrent.Await
import scala.concurrent.duration._

class NettySpec extends FlatSpec with Matchers with MockitoSugar {

  "Netty Transport" should "send and receive message correctly " in {
    val conf = TestUtil.DEFAULT_CONFIG
    val system = ActorSystem("transport", conf)
    val context = new Context(system, conf)
    val serverActor = TestProbe()(system)

    val port = Util.findFreePort()

    import system.dispatcher
    system.scheduler.scheduleOnce(Duration(1, TimeUnit.SECONDS)) {
      context.bind("server", new ActorLookupById {
        override def lookupLocalActor(id: Long): Option[ActorRef] = Some(serverActor.ref)
      }, false, port.get)
    }
    val client = context.connect(HostPort("127.0.0.1", port.get))

    val data = NettyMessage(0)
    val msg = new TaskMessage(0, 1, 2, data)
    client ! msg
    serverActor.expectMsg(15.seconds, data)

    context.close()
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }
}