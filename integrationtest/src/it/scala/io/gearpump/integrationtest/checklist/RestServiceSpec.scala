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

import io.gearpump.cluster.MasterToAppMaster
import io.gearpump.integrationtest.TestSpecBase

/**
 * The test spec checks REST service usage
 */
class RestServiceSpec extends TestSpecBase {

  "query system version" should {
    "retrieve the current version number" in {
      restClient.queryVersion() should not be empty
    }
  }

  "list applications" should {
    "retrieve 0 application after cluster just started" in {
      restClient.listRunningApps().size shouldEqual 0
    }

    "retrieve 1 application after the first application submission" in {
      // exercise
      val appId = restClient.submitApp(wordCountJar)
      expectAppIsRunning(appId, wordCountName)
      restClient.listRunningApps().size shouldEqual 1
    }
  }

  "submit application (wordcount)" should {
    "find a running application after submission" in {
      // exercise
      val appId = restClient.submitApp(wordCountJar)
      expectAppIsRunning(appId, wordCountName)
    }

    "reject a repeated submission request while the application is running" in {
      // setup
      var appId = restClient.submitApp(wordCountJar)
      expectAppIsRunning(appId, wordCountName)

      // exercise
      appId = restClient.submitApp(wordCountJar)
      appId shouldEqual -1
    }

    "reject an invalid submission (the jar file path is incorrect)" in {
      // exercise
      val appId = restClient.submitApp(wordCountJar + ".missing")
      appId shouldEqual -1
    }

    "find a running application with expected number of split and sum processors" in {
    }

    "find a running application with metrics value keeping changing" in {
    }
  }

  "kill application" should {
    "a running application should be killed" in {
      // setup
      val appId = restClient.submitApp(wordCountJar)
      expectAppIsRunning(appId, wordCountName)

      // exercise
      killAppAndVerify(appId)
    }

    "should fail when attempting to kill a stopped application" in {
      // setup
      val appId = restClient.submitApp(wordCountJar)
      expectAppIsRunning(appId, wordCountName)
      killAppAndVerify(appId)

      // exercise
      val success = restClient.killApp(appId)
      success shouldBe false
    }

    "should fail when attempting to kill a non-exist application" in {
      // setup
      val freeAppId = restClient.listApps().size + 1

      // exercise
      val success = restClient.killApp(freeAppId)
      success shouldBe false
    }
  }

  "cluster information" should {
    "retrieve 1 master for a non-HA cluster" in {

    }

    "retrieve 0 worker, if cluster is started without any workers" in {
      // todo: defect
    }

    "retrieve the same number of workers as cluster has" in {
    }

    "find a newly added worker instance" in {

    }
  }

  "configuration" should {
    "retrieve the configuration of master" in {

    }

    "retrieve the configuration of worker X" in {

    }

    "retrieve the configuration of executor X" in {

    }

    "retrieve the configuration of application X" in {

    }
  }

  "application life-cycle" should {
    "newly started application should be configured same as the previous one, after restart" in {

    }
  }

  private def killAppAndVerify(appId: Int): Unit = {
    val success = restClient.killApp(appId)
    success shouldBe true

    val actualApp = restClient.queryApp(appId)
    actualApp.appId shouldEqual appId
    actualApp.status shouldEqual MasterToAppMaster.AppMasterInActive
  }

}
