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

/**
 * The test spec checks REST service usage
 */
class RestServiceSpec extends TestSpecBase {

  "query system version" should "return the current version number" in {
    client.queryVersion should not be empty
  }

  "submit application (wordcount)" should "return status of running the application" in {
    // setup
    val jar = cluster.queryBuiltInExampleJars("wordcount-").head
    val appsCount = client.queryApps().size
    val appId = appsCount + 1

    // exercise
    val success = client.submitApp(jar)
    Thread.sleep(5000)
    success shouldEqual true
    val actual = client.queryApp(appId)

    // verify
    actual.appId shouldEqual appId
    actual.status shouldEqual "active"
    actual.appName shouldEqual "wordCount"
    killAppAndVerify(appId)
  }

  "submit same application twice" should "return an error at the second submission" in {
    // setup
    val jar = cluster.queryBuiltInExampleJars("wordcount-").head
    val appsCount = client.queryApps().size
    val appId = appsCount + 1

    // exercise
    var success = client.submitApp(jar)
    Thread.sleep(5000)
    success shouldEqual true
    val actual = client.queryApp(appId)

    // verify
    actual.appId shouldEqual appId
    actual.status shouldEqual "active"
    actual.appName shouldEqual "wordCount"

    // exercise
    success = client.submitApp(jar)
    Thread.sleep(5000)
    success shouldEqual false

    killAppAndVerify(appId)
  }

  private def killAppAndVerify(appId: Int): Unit = {
    client.killApp(appId)
    val actual = client.queryApp(appId)
    actual.appId shouldEqual appId
    actual.status shouldEqual "inactive"
  }

}
