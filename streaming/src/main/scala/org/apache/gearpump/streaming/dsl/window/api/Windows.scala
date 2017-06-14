/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.streaming.dsl.window.api

import java.time.Duration

/**
 * Defines how to apply window functions.
 *
 * @param windowFn how to divide windows
 * @param trigger when to trigger window result
 * @param accumulationMode whether to accumulate results across windows
 */
case class Windows(
    windowFn: WindowFunction,
    trigger: Trigger = EventTimeTrigger,
    accumulationMode: AccumulationMode = Discarding,
    description: String) {

  def triggering(trigger: Trigger): Windows = {
    Windows(windowFn, trigger, accumulationMode, description)
  }

  def accumulating: Windows = {
    Windows(windowFn, trigger, Accumulating, description)
  }

  def discarding: Windows = {
    Windows(windowFn, trigger, Discarding, description)
  }
}

object GlobalWindows {

  def apply(): Windows = {
    Windows(GlobalWindowFunction(), description = "globalWindows")
  }
}

object FixedWindows {

  /**
   * Defines a FixedWindow.
   *
   * @param size window size
   * @return a Window definition
   */
  def apply(size: Duration): Windows = {
    Windows(SlidingWindowFunction(size, size), description = "fixedWindows")
  }
}

object SlidingWindows {

  /**
   * Defines a SlidingWindow.
   *
   * @param size window size
   * @param step window step to slide forward
   * @return a Window definition
   */
  def apply(size: Duration, step: Duration): Windows = {
    Windows(SlidingWindowFunction(size, step), description = "slidingWindows")
  }
}

object SessionWindows {

  /**
   * Defines a SessionWindow.
   *
   * @param gap session gap
   * @return a Window definition
   */
  def apply(gap: Duration): Windows = {
    Windows(SessionWindowFunction(gap), description = "sessionWindows")
  }
}

