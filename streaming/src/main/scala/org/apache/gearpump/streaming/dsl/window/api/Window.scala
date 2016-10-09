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
 *
 * @param windowFn
 * @param trigger
 * @param accumulationMode
 */
case class Window(
    windowFn: WindowFn,
    trigger: Trigger = EventTimeTrigger,
    accumulationMode: AccumulationMode = Discarding) {

  def triggering(trigger: Trigger): Window = {
    Window(windowFn, trigger)
  }

  def accumulating: Window = {
    Window(windowFn, trigger, Accumulating)
  }

  def discarding: Window = {
    Window(windowFn, trigger, Discarding)
  }
}

object CountWindow {

  def apply(size: Int): Window = {
    Window(CountWindowFn(size), CountTrigger)
  }
}

object FixedWindow {

  /**
   * Defines a FixedWindow.
   * @param size window size
   * @return a Window definition
   */
  def apply(size: Duration): Window = {
    Window(SlidingWindowFn(size, size))
  }
}

object SlidingWindow {

  /**
   * Defines a SlidingWindow
   * @param size window size
   * @param step window step to slide forward
   * @return a Window definition
   */
  def apply(size: Duration, step: Duration): Window = {
    Window(SlidingWindowFn(size, step))
  }
}

