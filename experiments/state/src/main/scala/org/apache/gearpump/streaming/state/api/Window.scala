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

import scala.concurrent.duration.Duration


object Window {
  def apply(duration: Duration): Window = Window(duration, duration)
}

/**
 * interface to define window functions
 *
 * a Window will cover a time duration and move forward for each period
 * e.g. time ranges for a Window(5 seconds, 1 seconds) would be like
 * [0, 5), [1, 6), [2, 7), ...
 *
 * @param duration length of time covered by a window
 * @param period interval of window moving forward
 */
case class Window(duration: Duration, period: Duration)

