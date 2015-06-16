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

import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.source.DataSource

/**
 *
 * TimeReplayableSource would allow users to read and replay
 * messages from a startTime.
 *
 * The typical usage is like the following, where user get startTime
 * from TaskContext in onStart and read num of messages in each onNext.
 * User could optionally append a TimeStampFilter in case source messages
 * are not stored in TimeStamp order
 *
 * e.g.
 *   class UserTask(context: TaskContext, conf: Configs) extends Task(context, conf) {
 *     var startTime = 0L
 *
 *     override def onStart(context: TaskContext): Unit = {
 *       this.startTime = context.startTime
 *       TimeReplayableSource.setStartTime(this.startTime)
 *     }
 *
 *     override def onNext(msg: Message): Unit = {
 *       TimeReplayableSource.read(num).foreach { msg =>
 *         TimeStampFilter.filter(msg, this.startTime).map(output)
 *       }
 *     }
 *   }
 *
 */
trait TimeReplayableSource extends DataSource {
  def startFromBeginning(): Unit
  def setStartTime(startTime: TimeStamp): Unit
}


