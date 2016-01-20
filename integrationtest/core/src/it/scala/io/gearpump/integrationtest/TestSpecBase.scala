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
package io.gearpump.integrationtest

import io.gearpump.cluster.MasterToAppMaster
import io.gearpump.cluster.MasterToAppMaster.AppMasterData
import org.apache.log4j.Logger
import org.scalatest._

/**
 * The abstract test spec
 */
trait TestSpecBase extends WordSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  private val LOG = Logger.getLogger(getClass)

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (!MiniClusterProvider.managed) {
      LOG.info("Will test with a default standalone mini cluster")
      MiniClusterProvider.get.start()
    }
  }

  override def afterAll(): Unit = {
    if (!MiniClusterProvider.managed) {
      LOG.info("Will shutdown the default mini cluster")
      MiniClusterProvider.get.shutDown()
    }
    super.afterAll()
  }

  lazy val cluster = MiniClusterProvider.get
  lazy val commandLineClient = cluster.commandLineClient
  lazy val restClient = cluster.restClient

  lazy val wordCountJar = cluster.queryBuiltInExampleJars("wordcount-").head
  lazy val wordCountName = "wordCount"

  var restartClusterRequired: Boolean = false

  override def beforeEach() = {
    assert(cluster != null, "Configure MiniCluster properly in suite spec")
    cluster.isAlive shouldBe true
    restClient.listRunningApps().isEmpty shouldBe true
  }

  override def afterEach() = {
    if (restartClusterRequired || !cluster.isAlive) {
      restartClusterRequired = false
      LOG.info("Will restart the cluster for next test case")
      cluster.restart()
    } else {
      restClient.listRunningApps().foreach(app => {
        commandLineClient.killApp(app.appId) shouldBe true
      })
    }
  }

  def expectAppIsRunning(appId: Int, expectedAppName: String): AppMasterData = {
    val app = restClient.queryApp(appId)
    app.status shouldEqual MasterToAppMaster.AppMasterActive
    app.appName shouldEqual expectedAppName
    app
  }

}
