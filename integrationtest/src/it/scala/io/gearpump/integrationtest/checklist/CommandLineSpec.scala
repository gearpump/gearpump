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
import org.scalatest.BeforeAndAfter

/**
 * The test spec checks the command-line usage
 */
trait CommandLineSpec extends TestSpecBase with BeforeAndAfter {

  after {
    restClient.listActiveApps().foreach(app => {
      killAppAndVerify(app.appId)
    })
  }

  private def wordCountJar: String = {
    cluster.queryBuiltInExampleJars("wordcount-").head
  }
  private val wordCountName = "wordCount"

  "command `gear app`" should "submit an user application" in {
    // exercise
    val appId = expectSubmitAppSuccess(wordCountJar)

    // verify
    val actual = commandLineClient.queryApp(appId)
    actual should include(s"application: $appId, ")
    actual should include(s"name: $wordCountName, ")
    actual should include(s"status: ${MasterToAppMaster.AppMasterActive}")
  }

  "command `gear info`" should "return the same number of applications as REST api does" in {
    val expectedAppCount = restClient.listApps().size
    getAppsCount shouldEqual expectedAppCount

    val expectedAppId = getNextAvailableAppId
    val actualAppId = expectSubmitAppSuccess(wordCountJar)
    actualAppId shouldEqual expectedAppId
    getAppsCount shouldEqual expectedAppCount + 1
  }

  "command `gear app` submit same application twice" should "return an error at the second submission" in {
    // setup
    val appId = expectSubmitAppSuccess(wordCountJar)
    val expectedApp = commandLineClient.queryApp(appId)
    expectedApp should include(s"application: $appId, ")
    expectedApp should include(s"name: $wordCountName, ")
    expectedApp should include(s"status: ${MasterToAppMaster.AppMasterActive}")

    // exercise
    val success = commandLineClient.submitApp(wordCountJar)
    success shouldBe false
  }

  "command `gear app` submit wrong jar name" should "return an error" in {
    // exercise
    val success = commandLineClient.submitApp(wordCountJar + ".missing")
    success shouldBe false
  }

  "command `gear kill $app_id`" should "kill particular application" in {
    // setup
    val appId = expectSubmitAppSuccess(wordCountJar)

    // exercise
    val actual = commandLineClient.queryApp(appId)
    actual should include(s"application: $appId, ")
    actual should include(s"name: $wordCountName, ")
    actual should include(s"status: ${MasterToAppMaster.AppMasterActive}")
  }

  "command `gear kill $wrong_app_id`" should "return an error" in {
    // setup
    val freeAppId = getNextAvailableAppId

    // exercise
    val success = commandLineClient.killApp(freeAppId)
    success shouldBe false
  }

  "command `gear replay $app_id`" should "return replay the application from current min clock" in {
  }

  private def getAppsCount: Int = {
    val apps = commandLineClient.queryApps()
    apps should not be null
    apps.length
  }

  private def getNextAvailableAppId: Int = {
    getAppsCount + 1
  }

  private def expectSubmitAppSuccess(jar: String): Int = {
    //setup
    val appsCount = getAppsCount
    val appId = appsCount + 1

    //exercise
    val success = commandLineClient.submitApp(jar)
    success shouldBe true
    appId
  }

  private def killAppAndVerify(appId: Int): Unit = {
    commandLineClient.killApp(appId)
    val actual = commandLineClient.queryApp(appId)
    actual should include(s"application: $appId, ")
    actual should include(s"status: ${MasterToAppMaster.AppMasterInActive}")
  }

}
