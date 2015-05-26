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

package org.apache.gearpump.streaming.task

import java.util.Random

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.partitioner.{Partitioner, HashPartitioner}
import org.apache.gearpump.streaming.ProcessorDescription
import org.apache.gearpump.streaming.task.SubscriptionSpec.NextTask
import org.scalatest.{FlatSpec}

import org.mockito.Mockito._
import org.scalatest.{Matchers}
import org.scalatest.mock.MockitoSugar

class SubscriptionSpec extends FlatSpec with Matchers with MockitoSugar {
  val appId = 0
  val executorId = 0
  val taskId = TaskId(0, 0)
  val session = new Random().nextInt()

  val downstreamProcessorId = 1
  val partitioner = Partitioner[HashPartitioner]

  val parallism = 2
  val downstreamProcessor = ProcessorDescription(downstreamProcessorId, classOf[NextTask].getName, parallism)
  val subscriber = Subscriber(downstreamProcessorId, partitioner, downstreamProcessor.parallelism, downstreamProcessor.life)

  private def prepare: (Subscription, ExpressTransport) = {
    val transport = mock[ExpressTransport]
    val subscription = new Subscription(appId, executorId, taskId, subscriber, session, transport)
    subscription.start

    val expectedAckRequest = AckRequest(taskId, 0, session)
    verify(transport, times(1)).transport(expectedAckRequest, TaskId(1,0), TaskId(1, 1))

    (subscription, transport)
  }

  it should "broadcast message to all downstream tasks" in {
    val (subscription, transport) = prepare

    val broadcastMsg = LatencyProbe(System.currentTimeMillis())
    subscription.probeLatency(broadcastMsg)
    verify(transport, times(1)).transport(broadcastMsg, TaskId(1,0), TaskId(1, 1))
  }

  it should "send message and handle ack correctly" in {
    val (subscription, transport) = prepare
    val msg1 = new Message("1", timestamp = 70)
    subscription.sendMessage(msg1)

    verify(transport, times(1)).transport(msg1, TaskId(1,1))
    assert(subscription.minClock == 70)

    val msg2 = new Message("0", timestamp = 50)
    subscription.sendMessage(msg2)
    verify(transport, times(1)).transport(msg2, TaskId(1,0))

    // minClock has been set to smaller one
    assert(subscription.minClock == 50)

    val initialMinClock = subscription.minClock

    //ack initial AckRequest(0)
    subscription.receiveAck(Ack(TaskId(1, 1), 0, 0, session))
    subscription.receiveAck(Ack(TaskId(1, 0), 0, 0, session))

    //send 100 messages
    100 until 200 foreach {clock =>
      subscription.sendMessage(Message("1", clock))
      subscription.sendMessage(Message("2", clock))
    }

    // ack not received, minClock no change
    assert(subscription.minClock == initialMinClock)

    subscription.receiveAck(Ack(TaskId(1, 1), 100, 100, session))
    subscription.receiveAck(Ack(TaskId(1, 0), 100, 100, session))

    // ack received, minClock changed
    assert(subscription.minClock > initialMinClock)


    // we expect to receive two ackRequest for two downstream tasks
    val ackRequestForTask0 = AckRequest(taskId, 100, session)
    verify(transport, times(1)).transport(ackRequestForTask0, TaskId(1,0))

    val ackRequestForTask1 = AckRequest(taskId, 100, session)
    verify(transport, times(1)).transport(ackRequestForTask1, TaskId(1, 1))
  }

  it should "disallow more message sending if there is no ack back" in {
    val (subscription, transport) = prepare
    //send 100 messages
    0 until (Subscription.MAX_PENDING_MESSAGE_COUNT * 2 + 1) foreach {clock =>
      subscription.sendMessage(Message(randomMessage, clock))
    }

    assert(subscription.allowSendingMoreMessages() == false)

  }

  private def randomMessage: String = new Random().nextInt.toString

}

object SubscriptionSpec {


  class NextTask(taskContext : TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
    import taskContext.{output, self}

    override def onStart(startTime : StartTime) : Unit = {
    }

    override def onNext(msg : Message) : Unit = {
    }
  }
}