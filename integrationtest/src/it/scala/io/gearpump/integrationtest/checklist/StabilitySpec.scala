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
import io.gearpump.integrationtest.{TestSpecBase, Util}

import scala.concurrent.duration.Duration

/**
 * The test spec will perform destructive operations to check the stability
 */
class StabilitySpec extends TestSpecBase {

  "kill appmaster" should {
    "restart the whole application" in {
      // setup
      val appId = commandLineClient.submitApp(wordCountJar)
      val formerAppMaster = restClient.queryApp(appId).appMasterPath
      Util.retryUntil(restClient.queryStreamingAppDetail(appId).clock > 0)
      ensureClockStoredInMaster()

      // exercise
      restClient.killAppMaster(appId) shouldBe true
      // todo: how long master will begin to recover and how much time for the recovering?
      Util.retryUntil(restClient.queryApp(appId).appMasterPath != formerAppMaster)

      // verify
      val laterAppMaster = restClient.queryStreamingAppDetail(appId)
      laterAppMaster.status shouldEqual MasterToAppMaster.AppMasterActive
      laterAppMaster.clock should be > 0L
    }
  }

  "kill executor" should {
    "will create a new executor and application will replay from the latest application clock" in {
      // setup
      val appId = commandLineClient.submitApp(wordCountJar)
      Util.retryUntil(restClient.queryStreamingAppDetail(appId).clock > 0)
      val executorToKill = restClient.queryExecutorBrief(appId).map(_.executorId).max
      ensureClockStoredInMaster()

      // exercise
      restClient.killExecutor(appId, executorToKill) shouldBe true
      // todo: how long appmaster will begin to recover and how much time for the recovering?
      Util.retryUntil(restClient.queryExecutorBrief(appId).map(_.executorId).max > executorToKill)

      // verify
      val laterAppMaster = restClient.queryStreamingAppDetail(appId)
      laterAppMaster.status shouldEqual MasterToAppMaster.AppMasterActive
      laterAppMaster.clock should be > 0L
    }
  }

  "kill worker" should {
    "worker will not recover but all its executors will be migrated to other workers" in {
      // setup
      restartClusterRequired = true
      val appId = commandLineClient.submitApp(wordCountJar)
      Util.retryUntil(restClient.queryStreamingAppDetail(appId).clock > 0)
      val maximalExecutorId = restClient.queryExecutorBrief(appId).map(_.executorId).max
      val workerToKill = cluster.getWorkerHosts.head
      ensureClockStoredInMaster()

      // exercise
      cluster.removeWorkerNode(workerToKill)
      // todo: how long master will begin to recover and how much time for the recovering?
      Util.retryUntil(restClient.queryExecutorBrief(appId).map(_.executorId).max > maximalExecutorId)

      // verify
      val laterAppMaster = restClient.queryStreamingAppDetail(appId)
      laterAppMaster.status shouldEqual MasterToAppMaster.AppMasterActive
      laterAppMaster.clock should be > 0L
    }
  }

  "kill master" should {
    "master will be down and all workers will attempt to reconnect and suicide after X seconds" in {
      // setup
      restartClusterRequired = true
      val masters = cluster.getMasterHosts
      val config = restClient.queryMasterConfig()
      val shutDownTimeout = Duration(config.getString("master.akka.cluster.auto-down-unreachable-after"))

      // exercise
      masters.foreach(cluster.removeMasterNode)
      info(s"will sleep ${shutDownTimeout.toSeconds}s and then check workers are down")
      Thread.sleep(shutDownTimeout.toMillis)

      // verify
      val aliveWorkers = cluster.getWorkerHosts
      Util.retryUntil(aliveWorkers.forall(worker => !cluster.nodeIsOnline(worker)))
    }
  }

  private def ensureClockStoredInMaster(): Unit = {
    // todo: 5000ms is a fixed sync period in clock service. we wait for 5000ms to assume the clock is stored
    Thread.sleep(5000)
  }

}
