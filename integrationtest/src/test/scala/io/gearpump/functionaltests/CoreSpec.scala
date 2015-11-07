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
package io.gearpump.functionaltests

import io.gearpump.minicluster.MiniCluster
import org.scalatest.{FlatSpec, Matchers, BeforeAndAfterAll}

/**
 * The test spec contains basic functional test cases
 */
class CoreSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  private var cluster: MiniCluster = null
  private def client = cluster.client

  override def beforeAll(): Unit = {
    super.beforeAll()
    cluster = new MiniCluster
    cluster.start()
  }

  override def afterAll(): Unit = {
    cluster.shutDown()
    super.afterAll()
  }

  it should "return the current version number" in {
    client.queryVersion should not be empty
  }

  it should "submit a wordcount application" in {
    // fixture
    val jar = cluster.queryBuiltInExampleJars("wordcount-").head
    val splitNum = 2
    val sumNum = 4

    // setup
    val appsCount = client.queryApps().size
    val appId = appsCount + 1

    // exercise
    val success = client.submitApp(jar, s"-split $splitNum -sum $sumNum")
    Thread.sleep(5000)
    success shouldEqual true
    val actual = client.queryApp(appId)
    // todo: query as streaming app, so that we can verify the number of split and sum processors

    // verify
    actual.appId shouldEqual appId
    actual.status shouldEqual "active"
    actual.appName shouldEqual "wordCount"
    killAppAndVerify(appId)
  }

  private def killAppAndVerify(appId: Int): Unit = {
    client.killApp(appId)
    val actual = client.queryApp(appId)
    actual.appId shouldEqual appId
    actual.status shouldEqual "inactive"
  }

}
