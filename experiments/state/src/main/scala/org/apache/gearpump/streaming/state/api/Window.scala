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

package org.apache.gearpump.streaming.state.api

import org.apache.gearpump.TimeStamp

/**
 * a time window from (including)startClock until (excluding)endClock
 * @param startClock timestamp of data in the window is >= startClock
 * @param endClock   timestamp of data in the window is < startClock
 */
case class Window(startClock: TimeStamp, endClock: TimeStamp) extends Ordered[Window] {
  override def compare(that: Window): Int = {
      if (startClock < that.startClock) -1
      else if (startClock > that.startClock) 1
      else 0
  }
}

