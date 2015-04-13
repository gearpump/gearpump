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

import org.apache.gearpump.TimeStamp

/**
 *
 * a WindowManager manages windowNum of windows, and
 * each of them has the length of windowSize. WindowManager
 * move forward as time passes by its endTime
 * In each forward, WindowManager will move ahead by windowStride
 *
 * @param windowSize defined by Window(duration, period) and checkpoint interval
 *                   windowSize = GreatestCommonDivisor(checkpointInterval, duration, period)
 * @param windowNum defined by Window(duration, period), windowNum = duration / windowSize
 * @param windowStride defined by Window(duration, period), windowStride = duration / windowSize
 */
class WindowManager(windowSize: Long, windowNum: Long, windowStride: Long) extends ClockListener {
  // startTime of the earliest window
  private var startTime: Option[TimeStamp] = None
  // endTime of the latest window
  private var endTime: Option[TimeStamp] = None

  def this(windowSize: Long, windowNum: Long) = this(windowSize, windowNum, windowNum)

  def this(windowSize: Long) = this(windowSize, 1)

  override def syncTime(timestamp: TimeStamp): Unit = {
    super.syncTime(timestamp)
    if (startTime.isEmpty) {
      startTime = Some(CheckpointWindow.at(timestamp, windowSize).startTime)
      endTime = startTime.map(_ + windowSize * windowNum)
    }
  }

  def shouldForward: Boolean = {
    endTime.exists(_ <= getTime)
  }

  /**
   * forward by one windowStride
   */
  def forward(): Unit = {
    startTime = startTime.map(_ + windowSize * windowStride)
    endTime = endTime.map(_ + windowSize * windowStride)
  }

  /**
   * forward to contain the timestamp
   * @param timestamp
   */
  def forwardTo(timestamp: TimeStamp): Unit = {
    if (endTime.nonEmpty) {
      while (endTime.get <= timestamp) {
        forward()
      }
    }
  }

  /**
   * get all the managed windows
   * @return
   */
  def getWindows: Array[CheckpointWindow] = {
    if (startTime.nonEmpty && endTime.nonEmpty) {
      (for {
        time <- startTime.get until endTime.get by windowSize
      } yield CheckpointWindow(time, time + windowSize)).toArray
    } else {
      Array.empty[CheckpointWindow]
    }
  }

  /**
   * get a merged large window of all the managed windows
   * @return
   */
  def getMergedWindow: Option[CheckpointWindow] = {
    startTime.flatMap(st => endTime.map(et => CheckpointWindow(st, et)))
  }

  def getWindowSize = windowSize

}

