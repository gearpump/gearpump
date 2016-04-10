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

package io.gearpump.streaming.kafka.lib.consumer

/**
 * someone sleeps for exponentially increasing duration each time
 * until the cap
 *
 * @param backOffMultiplier The factor by which the duration increases.
 * @param initialDurationMs Time in milliseconds for initial sleep.
 * @param maximumDurationMs Cap up to which we will increase the duration.
 */
private[consumer] class ExponentialBackoffSleeper(
    backOffMultiplier: Double = 2.0,
    initialDurationMs: Long = 100,
    maximumDurationMs: Long = 10000) {

  require(backOffMultiplier > 1.0, "backOffMultiplier must be greater than 1")
  require(initialDurationMs > 0, "initialDurationMs must be positive")
  require(maximumDurationMs >= initialDurationMs, "maximumDurationMs must be >= initialDurationMs")

  private var sleepDuration = initialDurationMs

  def reset(): Unit = {
    sleepDuration = initialDurationMs
  }

  def sleep(): Unit = {
    Thread.sleep(sleepDuration)
    setNextSleepDuration()
  }

  def getSleepDuration: Long = sleepDuration

  def setNextSleepDuration(): Unit = {
    val next = (sleepDuration * backOffMultiplier).asInstanceOf[Long]
    sleepDuration = math.min(math.max(initialDurationMs, next), maximumDurationMs)
  }
}
