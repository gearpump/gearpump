/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package io.gearpump.streaming.kafka.lib.consumer

import org.scalatest.{WordSpec, Matchers}

class ExponentialBackoffSleeperSpec extends WordSpec with Matchers {

  "ExponentialBackOffSleeper" should {
    "sleep for increasing duration" in {
      val sleeper = new ExponentialBackoffSleeper(
        backOffMultiplier = 2.0,
        initialDurationMs = 100,
        maximumDurationMs = 10000
      )
      sleeper.getSleepDuration shouldBe 100
      sleeper.setNextSleepDuration()
      sleeper.getSleepDuration shouldBe 200
      sleeper.setNextSleepDuration()
      sleeper.getSleepDuration shouldBe 400
      sleeper.setNextSleepDuration()
      sleeper.getSleepDuration shouldBe 800
    }

    "sleep for no more than maximum duration" in {
      val sleeper = new ExponentialBackoffSleeper(
        backOffMultiplier = 2.0,
        initialDurationMs = 6400,
        maximumDurationMs = 10000
      )
      sleeper.getSleepDuration shouldBe 6400
      sleeper.setNextSleepDuration()
      sleeper.getSleepDuration shouldBe 10000
      sleeper.setNextSleepDuration()
      sleeper.getSleepDuration shouldBe 10000
    }

    "sleep for initial duration after reset" in {
      val sleeper = new ExponentialBackoffSleeper(
        backOffMultiplier = 2.0,
        initialDurationMs = 100,
        maximumDurationMs = 10000
      )
      sleeper.getSleepDuration shouldBe 100
      sleeper.setNextSleepDuration()
      sleeper.getSleepDuration shouldBe 200
      sleeper.reset()
      sleeper.getSleepDuration shouldBe 100
    }
  }
}


