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
 * AT-LEAST-ONCE API
 *
 * TimeReplayableSource would allow users to replay messages from the time
 * the system crashed on recovery, and thus provide at-least-once semantics.
 *
 * It is used in the same as [[DataSource]]
 *
 */
trait TimeReplayableSource extends DataSource {
  /**
   * this should be invoked in open() method or
   * before calling read() method of [[DataSource]]
   *
   * a startTime of None usually means start from beginning
   *
   * @param startTime is the start time of the system
   */
  def setStartTime(startTime: Option[TimeStamp]): Unit
}


