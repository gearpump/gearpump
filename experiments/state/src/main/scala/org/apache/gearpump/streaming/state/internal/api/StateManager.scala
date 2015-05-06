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

package org.apache.gearpump.streaming.state.internal.api

import org.apache.gearpump._
import org.apache.gearpump.streaming.state.api.{MonoidState, State}
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.immutable.TreeMap

/**
 * StateManager manages a state lifecycle for users
 * which includes
 *   1. init state at given time
 *   2. update state on new message
 *   3. checkpoint state in the background
 *      if checkpoint time is exceeded (refer to CheckpointManager)
 *
 * It maintains
 *   1. states divided by checkpoint window such that each message goes
 *      to its own window and update the state there
 *   2. state clock which advance on each checkpoint and will
 * affect a task's minClock
 *
 * There are two StateManagers defined now
 *   1. DefaultStateManager for general use
 *   2. WindowStateManager for window functions
 *
 */
private[internal] trait StateManager[T] {

  val state: MonoidState[T]
  val clockListeners: Array[ClockListener]
  val checkpointWindowSize: Long
  val checkpointManager: CheckpointManager

  private var stateClock: TimeStamp = 0L
  protected var states: TreeMap[CheckpointWindow, T] =
    TreeMap.empty[CheckpointWindow, T]

  private val LOG: Logger = LogUtil.getLogger(classOf[StateManager[T]])

  /**
   * init state at given time
   * @param timestamp
   */
  def init(timestamp: TimeStamp): Unit = {
    stateClock = timestamp
  }
  /**
   * 1. update internal state on new message
   * 2. checkpoint states exceeded by upstreamMinClock
   * 3. advance state clock
   */
  def update(msg: Message, msgDecoder: Message => T, upstreamMinClock: TimeStamp): Unit = {
    clockListeners.foreach(_.syncTime(upstreamMinClock))

    val timestamp = msg.timestamp
    val window = CheckpointWindow.at(timestamp, checkpointWindowSize)
    val data = msgDecoder(msg)

    updateState(window, timestamp, data)

    if (checkpointManager.shouldCheckpoint) {
      val checkpointTime = checkpointManager.getCheckpointTime.get
      val statesToCheckpoint =
        states
          .dropWhile(_._1.endTime <= stateClock)
          .takeWhile(_._1.endTime <= checkpointTime)
      checkpoint(statesToCheckpoint)
      stateClock = checkpointTime
    }
  }

  def checkpoint(states: TreeMap[CheckpointWindow, T]): Unit

  protected def dropStatesBefore(timestamp: TimeStamp): Unit = {
    states = states.dropWhile(_._1.endTime <= timestamp)
  }

  /**
   * report state clock to task
   * @return state clock
   */
  def minClock: TimeStamp = stateClock

  private[internal] def getStates: TreeMap[CheckpointWindow, T] = states

  private def updateState(window: CheckpointWindow, timestamp: TimeStamp, data: T): Unit = {
    states.get(window) match {
      case Some(st) =>
        states += window -> state.update(st, data)
      case None =>
        val maxClock = getMaxClock
        if (maxClock.isEmpty || timestamp >= maxClock.get) {
          states += window -> state.update(state.init, data)
        } else {
          LOG.warn(s"drop late message at $timestamp")
        }
    }
  }

  /**
   * @return endTime of latest window
   */
  private def getMaxClock: Option[TimeStamp] = {
    if (states.isEmpty) None
    else Option(states.maxBy(_._1)._1.endTime)
  }
}
