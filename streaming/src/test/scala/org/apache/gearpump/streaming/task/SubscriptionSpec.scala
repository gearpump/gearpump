/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.gearpump.streaming.task

import java.time.Instant
import java.util.Random

import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import org.apache.gearpump.{Message, Time}
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.partitioner.{HashPartitioner, Partitioner}
import org.apache.gearpump.streaming.source.Watermark
import org.apache.gearpump.streaming.task.SubscriptionSpec.NextTask
import org.apache.gearpump.streaming.{LifeTime, ProcessorDescription}

class SubscriptionSpec extends FlatSpec with Matchers with MockitoSugar {

  val appId = 0
  val executorId = 0
  val taskId = TaskId(0, 0)
  val session = new Random().nextInt()

  val downstreamProcessorId = 1
  val partitioner = Partitioner[HashPartitioner]

  val parallism = 2
  val downstreamProcessor = ProcessorDescription(downstreamProcessorId, classOf[NextTask].getName,
    parallism)
  val subscriber = Subscriber(downstreamProcessorId, partitioner, downstreamProcessor.parallelism,
    downstreamProcessor.life)

  private def prepare: (Subscription, TaskActor) = {
    val sender = mock[TaskActor]

    val subscription = new Subscription(appId, executorId, taskId, subscriber, session, sender)
    subscription.start()

    val expectedAckRequest = InitialAckRequest(taskId, session)
    verify(sender, times(1)).transport(expectedAckRequest, TaskId(1, 0), TaskId(1, 1))

    (subscription, sender)
  }

  it should "not send any more message when its life ends" in {
    val (subscription, _) = prepare
    subscription.changeLife(LifeTime(0, 0))
    val count = subscription.sendMessage(Message("some"))
    assert(count == 0)
  }

  it should "send message and handle ack correctly" in {
    val (subscription, sender) = prepare
    val msg1 = Message("1", timestamp = Instant.ofEpochMilli(70))
    when(sender.getProcessingWatermark).thenReturn(msg1.timestamp)
    // Send first message to Task(1, 1)
    subscription.sendMessage(msg1)

    verify(sender, times(1)).transport(msg1, TaskId(1, 1))
    assert(subscription.watermark == Time.MIN_TIME_MILLIS)

    val msg2 = Message("0", timestamp = Instant.ofEpochMilli(50))
    when(sender.getProcessingWatermark).thenReturn(msg2.timestamp)
    // Send first message to Task(1, 0)
    subscription.sendMessage(msg2)

    verify(sender, times(1)).transport(msg2, TaskId(1, 0))
    assert(subscription.watermark == Time.MIN_TIME_MILLIS)

    val initialMinClock = subscription.watermark

    // Acks initial AckRequest(0)
    subscription.receiveAck(Ack(TaskId(1, 1), 0, 0, session, Watermark.MIN.toEpochMilli))
    subscription.receiveAck(Ack(TaskId(1, 0), 0, 0, session, Watermark.MIN.toEpochMilli))

    // Sends 98 more messages to each downstream task
    100 until 198 foreach { clock =>
      subscription.sendMessage(Message("1", clock))
      subscription.sendMessage(Message("2", clock))
    }

    // Triger sending AckRequest
    val inOrders = org.mockito.Mockito.inOrder(sender, sender)

    val msg3 = Message("1", Instant.ofEpochMilli(200))
    val expectedAckRequest = AckRequest(taskId, 200, session, 200)
    when(sender.getProcessingWatermark).thenReturn(Instant.ofEpochMilli(200))
    subscription.sendMessage(msg3)
    inOrders.verify(sender).transport(msg3, TaskId(1, 1))
    inOrders.verify(sender).transport(expectedAckRequest, TaskId(1, 1))

    val msg4 = Message("2", Instant.ofEpochMilli(200))
    val expectedAckRequest2 = AckRequest(taskId, 200, session, 220)
    when(sender.getProcessingWatermark).thenReturn(Instant.ofEpochMilli(220))
    subscription.sendMessage(msg4)
    inOrders.verify(sender).transport(msg4, TaskId(1, 0))
    inOrders.verify(sender).transport(expectedAckRequest2, TaskId(1, 0))

    assert(subscription.watermark == initialMinClock)

    subscription.receiveAck(Ack(TaskId(1, 1), 200, 200, session, 200))
    subscription.receiveAck(Ack(TaskId(1, 0), 200, 200, session, 220))

    // Ack received, minClock changed
    assert(subscription.watermark == 200)
  }

  it should "disallow more message sending if there is no ack back" in {
    val (subscription, sender) = prepare
    // send 100 messages
    0 until (Subscription.MAX_PENDING_MESSAGE_COUNT * 2 + 1) foreach { clock =>
      when(sender.getProcessingWatermark).thenReturn(Watermark.MAX)
      subscription.sendMessage(Message(randomMessage, clock))
    }

    assert(!subscription.allowSendingMoreMessages())
  }

  private def randomMessage: String = new Random().nextInt.toString
}

object SubscriptionSpec {

  class NextTask(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  }
}