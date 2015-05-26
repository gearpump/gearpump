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

package org.apache.gearpump.streaming.state.impl

import org.apache.gearpump._
import org.apache.gearpump.streaming.state.api.{WindowDescription, State, CheckpointStore, StateSerializer}
import org.apache.gearpump.streaming.state.lib.serializer.WindowStateSerializer
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.immutable.{TreeSet, TreeMap}

object WindowState {
  val LOG: Logger = LogUtil.getLogger(classOf[WindowState[_]])
}

/**
 * manage states for window applications
 */
class WindowState[T](val stateSerializer: StateSerializer[T],
                     val checkpointInterval: TimeStamp,
                     val checkpointStore: CheckpointStore,
                     val taskContext: TaskContext,
                     windowDescription: WindowDescription) extends StateManager[T] with State[T] {
  import org.apache.gearpump.streaming.state.impl.WindowState._

  // TODO: add an option to keep only the recent N window states; otherwise, possibly OOM
  private var windowCheckpointStates: TreeMap[TimeStamp, T] = TreeMap.empty[TimeStamp, T]
  private var windowStates: TreeMap[TimeStamp, T] = TreeMap.empty[TimeStamp, T]
  private val windowStride: Long = windowDescription.stride.toMillis
  private val windowSize: Long = windowDescription.size.toMillis
  private val windowStateSerializer = new WindowStateSerializer[T](stateSerializer)

  override def recover(timestamp: TimeStamp): Unit = {
    super.recover(timestamp)
    checkpointStore.read(timestamp)foreach { bytes =>
      windowStates = windowStateSerializer.deserialize(bytes)
      windowCheckpointStates ++= windowStates
    }
    LOG.info(s"recovered $windowStates at $timestamp")
  }

  override def update(timestamp: TimeStamp, t: T, aggregate: (T, T) => T): Unit = {
    super.update(timestamp, t, aggregate)

    val checkpointTime = getCheckpointTime
    getWindows(timestamp).foreach { startTime =>
      if (timestamp < checkpointTime) {
        windowCheckpointStates += startTime -> windowCheckpointStates.get(startTime).fold(t)(aggregate(_, t))
      }

      windowStates += startTime -> windowStates.get(startTime).fold(t)(aggregate(_, t))
    }

  }

  override def checkpoint(timestamp: TimeStamp): Unit = {
    LOG.debug(s"checkpoint ($timestamp, $windowCheckpointStates) at $timestamp")
    checkpointStore.write(timestamp, windowStateSerializer.serialize(windowCheckpointStates))
    windowStates = windowStates.dropWhile(_._1 + windowSize <= timestamp)
    windowCheckpointStates = windowCheckpointStates.dropWhile(_._1 + windowSize <= timestamp)
    windowCheckpointStates ++= windowStates
  }

  override def get: Option[T] = {
    windowStates.lastOption.map(_._2)
  }

  private[impl] def getCheckpointStates: TreeMap[TimeStamp, T] = windowCheckpointStates

  private[impl] def getWindows(timestamp: TimeStamp): Array[TimeStamp] = {
    val first = timestamp < windowSize match {
      case true => 0L
      case false =>
        ((timestamp - windowSize) / windowStride  + 1) * windowStride
    }
    val last = timestamp / windowStride * windowStride
    first.to(last, windowStride).toArray
  }
}
