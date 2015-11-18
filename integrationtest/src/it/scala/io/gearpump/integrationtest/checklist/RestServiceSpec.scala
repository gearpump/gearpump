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
import io.gearpump.cluster.MasterToAppMaster.AppMasterData
import io.gearpump.integrationtest.TestSpecBase

/**
 * The test spec checks REST service usage
 */
trait RestServiceSpec extends TestSpecBase {

  "query system version" should "return the current version number" in {
    client.queryVersion() should not be empty
  }

  "submit application (wordcount)" should "return status of running the application" in {
    // setup
    val jar = cluster.queryBuiltInExampleJars("wordcount-").head
    val expectedAppName = "wordCount"
    client.listActiveApps().exists(_.appName == expectedAppName) shouldBe false

    // exercise
    val success = client.submitApp(jar)
    success shouldBe true
    val actualApp = queryActiveAppByNameAndVerify(expectedAppName)
    killAppAndVerify(actualApp.appId)
  }

  private def queryActiveAppByNameAndVerify(appName: String, timeout: Int = 15 * 1000): AppMasterData = {
    var app: Option[AppMasterData] = None
    var timeTook = 0
    while (app.isEmpty || timeTook > timeout) {
      app = client.listActiveApps().find(_.appName == appName)
      Thread.sleep(1000)
      timeTook += 1000
    }

    val actual = app.orNull
    actual should not be null
    actual.status shouldEqual MasterToAppMaster.AppMasterActive
    actual.appName shouldEqual appName
    actual
  }

  "submit same application twice" should "return an error at the second submission" in {
    // setup
    val jar = cluster.queryBuiltInExampleJars("wordcount-").head
    val expectedAppName = "wordCount"
    client.listActiveApps().exists(_.appName == expectedAppName) shouldBe false
    client.submitApp(jar) shouldBe true
    val expectedApp = queryActiveAppByNameAndVerify(expectedAppName)

    // exercise
    val success = client.submitApp(jar)
    success shouldBe false
    killAppAndVerify(expectedApp.appId)
  }

  private def killAppAndVerify(appId: Int): Unit = {
    client.killApp(appId)

    val actualApp = client.queryApp(appId)
    actualApp.appId shouldEqual appId
    actualApp.status shouldEqual MasterToAppMaster.AppMasterInActive
  }

}
