/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding cstateyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a cstatey of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.state.internal.impl

import org.apache.gearpump._
import org.apache.gearpump.streaming.state.internal.api._
import org.apache.gearpump.streaming.state.api.MonoidState
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.immutable.TreeMap

object WindowStateManager {
  val LOG: Logger = LogUtil.getLogger(classOf[WindowStateManager[_]])
}

/**
 * manage states for window applications
 */
class WindowStateManager[T](val state: MonoidState[T],
                            val checkpointWindowSize: Long,
                            val checkpointManager: CheckpointManager,
                            windowManager: WindowManager,
                            taskContext: TaskContext) extends StateManager[T] {

  import org.apache.gearpump.streaming.state.internal.impl.WindowStateManager._

  override val clockListeners: Array[ClockListener] = Array(checkpointManager, windowManager)

  override def init(timestamp: TimeStamp): Unit = {
    super.init(timestamp)
    windowManager.forwardTo(timestamp)
    windowManager.getWindows.foreach { window =>
      checkpointManager.recover(window.endTime).map(state.deserialize) match {
        case Some(st) => states += window -> st
        case None => states += window -> state.init
      }
    }
  }

  /**
   * 1. checkpoint states
   * 2. merge states in a window if the window should forward
   * 3. forward window
   * 4. drop states expired by the window
   */
  override def checkpoint(states: TreeMap[CheckpointWindow, T]): Unit = {
    states.foreach { case (win, st) =>
      checkpointManager.checkpoint(win.endTime, state.serialize(st))
      LOG.info(s"checkpoint (${win.endTime}, $st) at ${win.endTime}")
    }
    if (windowManager.shouldForward) {
      windowManager.getMergedWindow.foreach { win =>
        merge(win).foreach { case (mergedWin, mergedState) =>
          LOG.info(s"merged window $mergedWin; merged state $mergedState")
          taskContext.output(Message(state.serialize(mergedState), mergedWin.endTime))
        }

      }
      windowManager.forward()
      windowManager.getMergedWindow.foreach { win =>
        dropStatesBefore(win.startTime)
      }
    }
  }

  private[impl] def merge(window: CheckpointWindow): Option[(CheckpointWindow, T)] = {
    import org.apache.gearpump.streaming.state.internal.api.CheckpointWindow._
    val startTime = window.startTime
    val endTime = window.endTime
    val statesToMerge = states
      .dropWhile(_._1.endTime <= startTime)
      .takeWhile(_._1.endTime <= endTime)
    if (statesToMerge.isEmpty) {
      None
    } else {
      val merged = statesToMerge.reduce { (left, right) =>

        val (leftWindow, leftState) = left
        val (rightWindow, rightState) = right
        windowSemigroup.plus(leftWindow, rightWindow) -> state.update(leftState, rightState)
      }
      Some(merged)
    }
  }
}
