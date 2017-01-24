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
package org.apache.gearpump.integrationtest.checklist

import org.apache.gearpump.cluster.{ApplicationStatus, MasterToAppMaster}
import org.apache.gearpump.integrationtest.{TestSpecBase, Util}

/**
 * The test spec checks the command-line usage
 */
class CommandLineSpec extends TestSpecBase {

  "use `gear info` to list applications" should {
    "retrieve 0 application after cluster just started" in {
      // exercise
      getRunningAppCount shouldEqual 0
    }

    "retrieve 1 application after the first application submission" in {
      // setup
      val appId = expectSubmitAppSuccess(wordCountJar)
      expectAppIsRunningByParsingOutput(appId, wordCountName)

      // exercise
      getRunningAppCount shouldEqual 1
    }
  }

  "use `gear app` to submit application" should {
    "find a running application after submission" in {
      // exercise
      val appId = expectSubmitAppSuccess(wordCountJar)
      expectAppIsRunningByParsingOutput(appId, wordCountName)
    }

    "reject a repeated submission request while the application is running" in {
      // setup
      val appId = expectSubmitAppSuccess(wordCountJar)
      expectAppIsRunningByParsingOutput(appId, wordCountName)

      // exercise
      val actualAppId = commandLineClient.submitApp(wordCountJar)
      actualAppId shouldEqual -1
    }

    "reject an invalid submission (the jar file path is incorrect)" in {
      // exercise
      val actualAppId = commandLineClient.submitApp(wordCountJar + ".missing")
      actualAppId shouldEqual -1
    }
  }

  "use `gear kill` to kill application" should {
    "a running application should be killed" in {
      // setup
      val appId = expectSubmitAppSuccess(wordCountJar)

      // exercise
      val success = commandLineClient.killApp(appId)
      success shouldBe true
    }

    "should fail when attempting to kill a stopped application" in {
      // setup
      val appId = expectSubmitAppSuccess(wordCountJar)
      var success = commandLineClient.killApp(appId)
      success shouldBe true

      // exercise
      success = commandLineClient.killApp(appId)
      success shouldBe false
    }

    "the EmbeddedCluster can be used as embedded cluster in process" in {
      // setup
      val args = "-debug true -sleep 10"
      val appId = expectSubmitAppSuccess(wordCountJar, args)
      val success = commandLineClient.killApp(appId)
      success shouldBe true
    }

    "should fail when attempting to kill a non-exist application" in {
      // setup
      val freeAppId = getNextAvailableAppId

      // exercise
      val success = commandLineClient.killApp(freeAppId)
      success shouldBe false
    }
  }

  "use `gear replay` to replay the application from current min clock" should {
    "todo: description" in {
      // todo: test code
    }
  }

  private def getRunningAppCount: Int = {
    commandLineClient.listRunningApps().length
  }

  private def getNextAvailableAppId: Int = {
    commandLineClient.listApps().length + 1
  }

  private def expectSubmitAppSuccess(jar: String, args: String = ""): Int = {
    val appId = commandLineClient.submitApp(jar)
    appId should not equal -1
    appId
  }

  private def expectAppIsRunningByParsingOutput(appId: Int, expectedName: String): Unit = {
    Util.retryUntil(() => {
      val actual = commandLineClient.queryApp(appId)
      actual.contains(s"application: $appId, ") &&
        actual.contains(s"name: $expectedName, ") &&
        actual.contains(s"status: ${ApplicationStatus.ACTIVE.status}")
    }, "application is running")
  }
}
