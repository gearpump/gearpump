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

package org.apache.gearpump.streaming.transaction.api

import org.apache.gearpump.{Message, TimeStamp}

/**
 *
 * TimeReplayableSource would allow users to pull and replay
 * messages from a startTime.
 *
 * The typical usage is like the following, where user get startTime
 * from TaskContext in onStart and pull num of messages in each onNext.
 * User could optionally append a TimeStampFilter in case source messages
 * are not stored in TimeStamp order
 *
 * e.g.
 *   class UserTask(conf: Configs) extends TaskActor(conf) {
 *     var startTime = 0L
 *
 *     override def onStart(context: TaskContext): Unit = {
 *       this.startTime = context.startTime
 *       TimeReplayableSource.setStartTime(this.startTime)
 *     }
 *
 *     override def onNext(msg: Message): Unit = {
 *       TimeReplayableSource.pull(num).foreach { msg =>
 *         TimeStampFilter.filter(msg, this.startTime).map(output)
 *       }
 *     }
 *   }
 *
 */
trait TimeReplayableSource extends java.io.Serializable {
  def startFromBeginning(): Unit
  def setStartTime(startTime: TimeStamp): Unit
  /**
   *  pull the num of messages from source
   *  Note: this is best effort. The returned
   *  message count may be less than num
   */
  def pull(num: Int): List[Message]

  def close(): Unit
}


