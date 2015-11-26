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

package io.gearpump.experiments.storm.producer

import java.util.{List => JList}

import backtype.storm.spout.{ISpout, ISpoutOutputCollector}
import io.gearpump.TimeStamp
import io.gearpump.experiments.storm.util.StormOutputCollector
import io.gearpump.util.LogUtil
import org.slf4j.Logger

case class PendingMessage(id: Object, messageTime: TimeStamp, startTime: TimeStamp)

/**
 * this is used by Storm Spout to emit messages
 */
private[storm] class StormSpoutOutputCollector(
    collector: StormOutputCollector, spout: ISpout, ackEnabled: Boolean) extends ISpoutOutputCollector {
  private var pendingMessage: Option[PendingMessage] = None

  override def emit(streamId: String, values: JList[AnyRef], messageId: Object): JList[Integer] = {
    val curTime = System.currentTimeMillis()
    collector.setTimestamp(curTime)
    val outTasks = collector.emit(streamId, values)
    setPendingOrAck(messageId, curTime, curTime)
    outTasks
  }

  override def reportError(throwable: Throwable): Unit = {
    throw throwable
  }

  override def emitDirect(taskId: Int, streamId: String, values: JList[AnyRef], messageId: Object): Unit = {
    val curTime = System.currentTimeMillis()
    collector.setTimestamp(curTime)
    collector.emitDirect(taskId, streamId, values)
    setPendingOrAck(messageId, curTime, curTime)
  }


  def ackPendingMessage(checkpointClock: TimeStamp): Unit = {
    pendingMessage.foreach { case PendingMessage(id, messageTime, _) =>
      if (messageTime <= checkpointClock) {
        spout.ack(id)
        reset
      }
    }
  }

  def failPendingMessage(timeoutMillis: Long): Unit = {
    pendingMessage.foreach { case PendingMessage(id, _, startTime) =>
      if (System.currentTimeMillis() - startTime >= timeoutMillis) {
        spout.fail(id)
        reset
      }
    }
  }

  private def reset: Unit = {
    pendingMessage = None
  }

  private def setPendingOrAck(messageId: Object, startTime: TimeStamp, messageTime: TimeStamp): Unit = {
    if (ackEnabled && pendingMessage.isEmpty) {
      pendingMessage = Some(PendingMessage(messageId, messageTime, startTime))
    } else {
      spout.ack(messageId)
    }
  }
}
