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
  * The test spec checks the command-line usage
  */
class CommandLineSpec extends TestSpecBase {

  private def wordCountJar =
    cluster.queryBuiltInExampleJars("wordcount-").head

  private val wordCountName = "wordCount"

  "use `gear app` to submit application" should {
    "return its running status" in {
      // exercise
      val appId = expectSubmitAppSuccess(wordCountJar)
      expectAppIsRunning(appId, wordCountName)
    }
  }

  "use `gear info` to list applications" should {
    "return the same number as REST api does" in {
      val expectedAppCount = restClient.listApps().size
      getAppsCount shouldEqual expectedAppCount

      val expectedAppId = getNextAvailableAppId
      val actualAppId = expectSubmitAppSuccess(wordCountJar)
      actualAppId shouldEqual expectedAppId
      getAppsCount shouldEqual expectedAppCount + 1
    }
  }

  "use `gear app` to submit same application twice" should {
    "return an error at the second submission" in {
      // setup
      val appId = expectSubmitAppSuccess(wordCountJar)
      expectAppIsRunning(appId, wordCountName)

      // exercise
      val actualAppId = commandLineClient.submitApp(wordCountJar)
      actualAppId shouldEqual -1
    }
  }

  "use `gear app` to submit non-exist application" should {
    "return an error" in {
      // exercise
      val actualAppId = commandLineClient.submitApp(wordCountJar + ".missing")
      actualAppId shouldEqual -1
    }
  }

  "use `gear kill` to kill application" should {
    "be successful" in {
      // setup
      val appId = expectSubmitAppSuccess(wordCountJar)

      // exercise
      val success = commandLineClient.killApp(appId)
      success shouldBe true
    }
  }

  "use `gear kill` to kill a non-exist application" should {
    "return an error" in {
      // setup
      val freeAppId = getNextAvailableAppId

      // exercise
      val success = commandLineClient.killApp(freeAppId)
      success shouldBe false
    }
  }

  "use `gear replay` to replay the application from current min clock" should {
    "be successful" in {
    }
  }

  private def getAppsCount: Int = {
    val apps = commandLineClient.listApps()
    apps should not be null
    apps.length
  }

  private def getNextAvailableAppId = getAppsCount + 1

  private def expectSubmitAppSuccess(jar: String): Int = {
    val appId = commandLineClient.submitApp(jar)
    appId should not equal -1
    appId
  }

  private def expectAppIsRunning(appId: Int, expectedName: String): Unit = {
    val actual = commandLineClient.queryApp(appId)
    actual should include(s"application: $appId, ")
    actual should include(s"name: $expectedName, ")
    actual should include(s"status: ${MasterToAppMaster.AppMasterActive}")
  }

}
