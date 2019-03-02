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

package io.gearpump.util

import akka.actor._
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import com.github.ghik.silencer.silent
import io.gearpump.cluster.TestUtil
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import scala.concurrent.duration._

class TimeOutSchedulerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("WorkerSpec", TestUtil.DEFAULT_CONFIG))
  val mockActor = TestProbe()

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "The TimeOutScheduler" should {
    "handle the time out event" in {
      val testActorRef = TestActorRef(Props(classOf[TestActor], mockActor.ref))
      @silent
      val testActor = testActorRef.underlyingActor.asInstanceOf[TestActor]
      testActor.sendMsgToIgnore()
      mockActor.expectMsg(30.seconds, MessageTimeOut)
    }
  }
}

case object Echo
case object MessageTimeOut

class TestActor(mock: ActorRef) extends Actor with TimeOutScheduler {

  val target = context.actorOf(Props(classOf[EchoActor]))

  override def receive: Receive = {
    case _ =>
  }

  def sendMsgToIgnore(): Unit = {
    sendMsgWithTimeOutCallBack(target, Echo, 2000, sendMsgTimeOut())
  }

  private def sendMsgTimeOut(): Unit = {
    mock ! MessageTimeOut
  }
}

class EchoActor extends Actor {
  override def receive: Receive = {
    case _ =>
  }
}
