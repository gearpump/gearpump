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
package io.gearpump.util

import akka.actor._
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import io.gearpump.cluster.TestUtil
import io.gearpump.cluster.TestUtil
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.slf4j.Logger
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
      val testActor = testActorRef.underlyingActor.asInstanceOf[TestActor]
      testActor.sendMsgToReply()
      mockActor.expectMsg(Echo)
      testActor.sendMsgToIgnore()
      mockActor.expectMsg(30 seconds, MessageTimeOut)
    }
  }
}

case object Echo
case object MessageToReply
case object MessageTimeOut

class TestActor(mock: ActorRef) extends Actor with TimeOutScheduler {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  val target = context.actorOf(Props(classOf[EchoActor]))

  override def receive: Receive = {
    case Echo =>
      mock ! Echo
  }

  def sendMsgToReply(): Unit = {
    sendMsgWithTimeOutCallBack(target, MessageToReply, 2, ())
  }
  
  def sendMsgToIgnore(): Unit = {
    sendMsgWithTimeOutCallBack(target, Echo, 2, sendMsgTimeOut())
  }
  
  private def sendMsgTimeOut(): Unit = {
    mock ! MessageTimeOut
  }
}

class EchoActor extends Actor {
  override def receive: Receive = {
    case MessageToReply =>
      sender ! Echo
    case _ =>
  }
}
