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
package io.gearpump.integrationtest.checklist

import io.gearpump.integrationtest.{Util, TestSpecBase}

/**
 * The test spec checks the compatibility of running Storm applications
 */
class StormCompatibilitySpec extends TestSpecBase {

  private val STORM_STARTER_JAR = "/opt/gearpump/lib/storm/storm-starter-0.9.5.jar"

  "run storm over gearpump applications" should {
    "succeed" in {
      // exercise
      val appId = commandLineClient.submitStormApp(STORM_STARTER_JAR,
        "storm.starter.ExclamationTopology exclamation")
      Thread.sleep(5000)

      // verify
      val actual = expectAppIsRunning(appId, "exclamation")
      Util.retryUntil(restClient.queryStreamingAppDetail(actual.appId).clock > 0)
    }
  }

  "multilang storm over gearpump" should {
    "support Python" in {

    }
  }

  "storm over gearpump" should {
    "support DRPC" in {

    }

    "support at-least-once semantics with Kafka" in {

    }

    "support tick tuple" in {
      // exercise
      val appId = commandLineClient.submitStormApp(STORM_STARTER_JAR,
        "storm.starter.RollingTopWords slidingWindowCounts remote")
      Thread.sleep(5000)

      // verify
      val actual = expectAppIsRunning(appId, "slidingWindowCounts")
      Util.retryUntil(restClient.queryStreamingAppDetail(actual.appId).clock > 0)
    }
  }

}
