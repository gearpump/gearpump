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

import io.gearpump.integrationtest.TestSpecBase
import io.gearpump.integrationtest.minicluster.Util
import io.gearpump.integrationtest.storm.StormClient

/**
 * The test spec checks the compatibility of running Storm applications
 */
class StormCompatibilitySpec extends TestSpecBase {

  private val stormClient = new StormClient(cluster, commandLineClient)

  override def beforeAll(): Unit = {
    super.beforeAll()
    stormClient.start()
  }

  override def afterAll(): Unit = {
    stormClient.shutDown()
    super.afterAll()
  }

  "Storm over Gearpump" should {
    "support basic topologies" in {
      // exercise
      val appId = stormClient.submitStormApp(
        mainClass = "storm.starter.ExclamationTopology", args = "exclamation")
      Thread.sleep(5000)

      // verify
      val actual = expectAppIsRunning(appId, "exclamation")
      Util.retryUntil(restClient.queryStreamingAppDetail(actual.appId).clock > 0)
    }

    "provide multilang support in python" in {
      // commented out till python support is added to docker image
/*      // exercise
      val appId = stormClient.submitStormApp(config = "",
        mainClass = "storm.starter.WordCountTopology", args = "wordcount")
      Thread.sleep(5000)

      // verify
      val actual = expectAppIsRunning(appId, "wordcount")
      Util.retryUntil(restClient.queryStreamingAppDetail(actual.appId).clock > 0)*/
    }

    "support DRPC" in {
      stormClient.submitStormApp(mainClass = "storm.starter.ReachTopology",
        args = "reach")
      val drpcClient = stormClient.getDRPCClient
      drpcClient.execute("reach", "foo.com/blog/1") shouldBe "16"
      drpcClient.execute("reach", "engineering.twitter.com/blog/5") shouldBe "14"
      drpcClient.execute("reach", "notaurl.com") shouldBe "0"
    }

    "support tick tuple" in {
      // exercise
      val appId = stormClient.submitStormApp(mainClass = "storm.starter.RollingTopWords",
        args = "slidingWindowCounts remote")
      Thread.sleep(5000)

      // verify
      val actual = expectAppIsRunning(appId, "slidingWindowCounts")
      Util.retryUntil(restClient.queryStreamingAppDetail(actual.appId).clock > 0)
    }

    "support at-least-once semantics with Kafka" in {

    }
  }

}
