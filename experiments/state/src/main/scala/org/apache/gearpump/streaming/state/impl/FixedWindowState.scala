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

package org.apache.gearpump.streaming.state.impl

import org.apache.gearpump._
import org.apache.gearpump.streaming.state.api.{CheckpointManager, State, StateOp, Window}
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.immutable.TreeMap


object FixedWindowState {
  val LOG: Logger = LogUtil.getLogger(classOf[FixedWindowState[_]])
}
/**
 *  each fixed window has it own state
 *  a message only affect state of the window it belongs to
 */
class FixedWindowState[T](windowSize: Long,
                          op: StateOp[T],
                          checkpointManager: CheckpointManager,
                          taskContext: TaskContext) extends State[T] {
  import org.apache.gearpump.streaming.state.impl.FixedWindowState._

  // state clock is always aligned with a window's endClock
  private var stateClock = 0L
  private var states = TreeMap.empty[Window, T]

  /**
   * just create a new window starting at the timestamp
   * which should be the endClock of the latest checkpointed window
   * @param timestamp
   */
  override def init(timestamp: TimeStamp): Unit = {
    stateClock = timestamp
    val window = Window(timestamp, timestamp + windowSize)
    states += window -> op.init
  }

  /**
   * update the state of an existing window which satisfies
   * startClock <= msg.timestamp < endClock
   * or create a new window if msg.timestamp exceeds the largest endClock
   * checkpointed state will be sent down streams
   * @param msg
   */
  override def update(msg: Message): Unit = {
    checkpointManager.syncTime(taskContext.upstreamMinClock)
    val timestamp = msg.timestamp
    val window = getWindow(timestamp)
    states.get(window) match {
      case Some(st) =>
        states += window -> op.update(msg, st)
      case None =>
        val maxClock = getMaxClock
        if (maxClock.isEmpty || timestamp >= maxClock.get) {
          states += window -> op.update(msg, op.init)
        } else {
          LOG.warn(s"receive stale message $msg")
        }
    }
    if (checkpointManager.shouldCheckpoint) {
      val checkpointTime = checkpointManager.getCheckpointTime
      val finishedStates = states.takeWhile(_._1.endClock <= checkpointTime.get)
      finishedStates.foreach { case (win, st) =>
        LOG.info(s"checkpoint (${win.endClock}, $st) at $checkpointTime")
        checkpointManager.checkpoint(win.endClock, op.serialize(st))
        stateClock = win.endClock
        taskContext.output(Message((win.endClock, st), win.endClock))
        states -= win
      }
    }
  }

  override def minClock: TimeStamp = stateClock

  /**
   * get the state of the latest window
   * @return
   */
  override def unwrap: Option[T] = {
    states.lastOption.map(_._2)
  }

  /**
   * calculate the window clocks a message should go to
   * @param timestamp
   * @return
   */
  private[impl] def getWindow(timestamp: TimeStamp): Window = {
    val startClock = (timestamp / windowSize) * windowSize
    val endClock = startClock + windowSize
    Window(startClock, endClock)
  }

  /**
   * @return endClock of latest window
   */
  private[impl] def getMaxClock: Option[TimeStamp] = {
    if (states.isEmpty) None
    else Option(states.maxBy(_._1)._1.endClock)
  }
}
