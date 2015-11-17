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
import io.gearpump.integrationtest.minicluster.CommandClient

/**
 * The test spec checks the command-line usage
 */
trait CommandLineSpec extends TestSpecBase {

  val commandClient = new CommandClient(cluster.getMasterHosts.head)

  "command `gear app`" should "submit an user application" in {
    //submit
    val jar = cluster.queryBuiltInExampleJars("wordcount-").head
    val appId = submitAppAndVerify(jar)

    //verify
    val actual = commandClient.queryApp(appId)
    actual.contains(s"application: $appId, ") shouldEqual true
    actual.contains(s"name: wordCount, ") shouldEqual true
    actual.contains(s"status: active") shouldEqual true

    killAppAndVerify(appId)
  }

  "command `gear info`" should "return particular application runtime information" in {
    //submit
    val jar = cluster.queryBuiltInExampleJars("wordcount-").head
    val appId = submitAppAndVerify(jar)

    //verify
    val appsCount = commandClient.queryApps().length
    appsCount shouldEqual appId
    val actual = commandClient.queryApp(appId)
    actual.contains(s"application: $appId, ") shouldEqual true
    actual.contains(s"status: active") shouldEqual true
    actual.contains(s"name: wordCount, ") shouldEqual true

    killAppAndVerify(appId)
  }

  "command `gear app` submit same application twice" should "return an error at the second submission" in {
    //submit
    val jar = cluster.queryBuiltInExampleJars("wordcount-").head
    val appId = submitAppAndVerify(jar)

    //verify
    val actual = commandClient.queryApp(appId)
    actual.contains(s"application: $appId, ") shouldEqual true
    actual.contains(s"name: wordCount, ") shouldEqual true
    actual.contains(s"status: active") shouldEqual true

    //submit twice
    val success = commandClient.submitApp(jar)
    Thread.sleep(5000)
    success shouldEqual false

    killAppAndVerify(appId)
  }

  "command `gear app` submit wrong jar name" should "return an error" in {
    val jar = cluster.queryBuiltInExampleJars("wordcount-").head.replace("/", ".")

    //exercise
    val success = commandClient.submitApp(jar)
    Thread.sleep(5000)
    success shouldEqual false
  }

  "command `gear kill $app_id`" should "kill particular application" in {
    //submit
    val jar = cluster.queryBuiltInExampleJars("wordcount-").head
    val appId = submitAppAndVerify(jar)

    //verify
    val actual = commandClient.queryApp(appId)
    actual.contains(s"application: $appId, ") shouldEqual true
    actual.contains(s"status: active") shouldEqual true

    killAppAndVerify(appId)
  }

  "command `gear kill $wrong_app_id`" should "return an error" in {
    //submit
    val jar = cluster.queryBuiltInExampleJars("wordcount-").head
    val appId = submitAppAndVerify(jar)

    //kill wrong appId and verify
    commandClient.killApp(appId + 1)
    Thread.sleep(5000)
    val actual = commandClient.queryApp(appId)
    actual.contains(s"application: $appId, ") shouldEqual true
    actual.contains(s"status: active") shouldEqual true

    killAppAndVerify(appId)
  }

  "command `gear replay $app_id`" should "return replay the application from current min clock" in {
  }

  private def submitAppAndVerify(jar: String): Int = {
    //setup
    val appsCount = commandClient.queryApps().length
    val appId = appsCount + 1

    //exercise
    val success = commandClient.submitApp(jar)
    Thread.sleep(5000)
    success shouldEqual true
    appId
  }

  private def killAppAndVerify(appId: Int): Unit = {
    commandClient.killApp(appId)
    Thread.sleep(5000)
    val actual = commandClient.queryApp(appId)
    actual.contains(s"application: $appId, ") shouldEqual true
    actual.contains(s"status: inactive") shouldEqual true
  }

}
