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
package org.apache.gearpump.integrationtest

import org.scalatest._
import org.apache.gearpump.cluster.{ApplicationStatus, MasterToAppMaster}
import org.apache.gearpump.cluster.MasterToAppMaster.AppMasterData
import org.apache.gearpump.util.LogUtil

/**
 * The abstract test spec
 */
trait TestSpecBase
  extends WordSpec with Matchers with BeforeAndAfterEachTestData with BeforeAndAfterAll {

  private def LOGGER = LogUtil.getLogger(getClass)

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (!MiniClusterProvider.managed) {
      LOGGER.info("Will test with a default standalone mini cluster")
      MiniClusterProvider.get.start()
    }
  }

  override def afterAll(): Unit = {
    if (!MiniClusterProvider.managed) {
      LOGGER.info("Will shutdown the default mini cluster")
      MiniClusterProvider.get.shutDown()
    }
    super.afterAll()
  }

  lazy val cluster = MiniClusterProvider.get
  lazy val commandLineClient = cluster.commandLineClient
  lazy val restClient = cluster.restClient

  lazy val wordCountJar = cluster.queryBuiltInExampleJars("wordcount_").head
  lazy val wordCountName = "wordCount"
  lazy val wordCountClass = "org.apache.gearpump.streaming.examples.wordcount.WordCount"

  var restartClusterRequired: Boolean = false

  override def beforeEach(td: TestData): Unit = {

    LOGGER.debug(s">### =============================================================")
    LOGGER.debug(s">###1 Prepare test: ${td.name}\n")

    assert(cluster != null, "Configure MiniCluster properly in suite spec")
    cluster.isAlive shouldBe true
    restClient.listPendingOrRunningApps().isEmpty shouldBe true
    LOGGER.debug(s">### =============================================================")
    LOGGER.debug(s">###2 Start test: ${td.name}\n")
  }

  override def afterEach(td: TestData): Unit = {
    LOGGER.debug(s"<### =============================================================")
    LOGGER.debug(s"<###3 End test: ${td.name}\n")

    if (restartClusterRequired || !cluster.isAlive) {
      restartClusterRequired = false
      LOGGER.info("Will restart the cluster for next test case")
      cluster.restart()
    } else {
      restClient.listPendingOrRunningApps().foreach(app => {
        commandLineClient.killApp(app.appId) shouldBe true
      })
    }
  }

  def expectAppIsRunning(appId: Int, expectedAppName: String): Unit = {
    Util.retryUntil(() => {
      val app = restClient.queryApp(appId)
      app.status == ApplicationStatus.ACTIVE && app.appName == expectedAppName
    }, s"$expectedAppName is running")
  }
}