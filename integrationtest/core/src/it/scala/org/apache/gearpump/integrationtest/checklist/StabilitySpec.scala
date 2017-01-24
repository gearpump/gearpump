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

import scala.concurrent.duration.Duration
import org.apache.gearpump.cluster.{ApplicationStatus, MasterToAppMaster}
import org.apache.gearpump.cluster.worker.WorkerId
import org.apache.gearpump.integrationtest.{TestSpecBase, Util}
import org.apache.gearpump.util.{Constants, LogUtil}

/**
 * The test spec will perform destructive operations to check the stability. Operations
 * contains shutting-down appmaster, executor, or worker, and etc..
 */
class StabilitySpec extends TestSpecBase {

  private val LOG = LogUtil.getLogger(getClass)

  "kill appmaster" should {
    "restart the whole application" in {
      // setup
      val appId = commandLineClient.submitApp(wordCountJar)
      val formerAppMaster = restClient.queryApp(appId).appMasterPath
      Util.retryUntil(() => restClient.queryStreamingAppDetail(appId).clock > 0, "app running")
      ensureClockStoredInMaster()

      // exercise
      restClient.killAppMaster(appId) shouldBe true
      // todo: how long master will begin to recover and how much time for the recovering?
      Util.retryUntil(() => restClient.queryApp(appId).appMasterPath != formerAppMaster,
        "appmaster killed and restarted")

      // verify
      val laterAppMaster = restClient.queryStreamingAppDetail(appId)
      laterAppMaster.status shouldEqual ApplicationStatus.ACTIVE
      laterAppMaster.clock should be > 0L
    }
  }

  "kill executor" should {
    "will create a new executor and application will replay from the latest application clock" in {
      // setup
      val appId = commandLineClient.submitApp(wordCountJar)
      Util.retryUntil(() => restClient.queryStreamingAppDetail(appId).clock > 0, "app running")
      val executorToKill = restClient.queryExecutorBrief(appId).map(_.executorId).max
      ensureClockStoredInMaster()

      // exercise
      restClient.killExecutor(appId, executorToKill) shouldBe true
      // todo: how long appmaster will begin to recover and how much time for the recovering?
      Util.retryUntil(() => restClient.queryExecutorBrief(appId)
        .map(_.executorId).max > executorToKill,
        s"executor $executorToKill killed and restarted")

      // verify
      val laterAppMaster = restClient.queryStreamingAppDetail(appId)
      laterAppMaster.status shouldEqual ApplicationStatus.ACTIVE
      laterAppMaster.clock should be > 0L
    }
  }

  private def hostName(workerId: WorkerId): String = {
    val worker = restClient.listRunningWorkers().find(_.workerId == workerId)
    // Parse hostname from JVM info (in format: PID@hostname)
    val hostname = worker.get.jvmName.split("@")(1)
    hostname
  }

  "kill worker" should {
    "worker will not recover but all its executors will be migrated to other workers" in {
      // setup
      restartClusterRequired = true
      val appId = commandLineClient.submitApp(wordCountJar)
      Util.retryUntil(() => restClient.queryStreamingAppDetail(appId).clock > 0, "app running")

      val allexecutors = restClient.queryExecutorBrief(appId)
      val maxExecutor = allexecutors.sortBy(_.executorId).last
      ensureClockStoredInMaster()

      val appMaster = allexecutors.find(_.executorId == Constants.APPMASTER_DEFAULT_EXECUTOR_ID)

      LOG.info(s"Max executor Id is executor: ${maxExecutor.executorId}, " +
        s"worker: ${maxExecutor.workerId}")
      val executorsSharingSameWorker = allexecutors
        .filter(_.workerId == maxExecutor.workerId).map(_.executorId)
      LOG.info(s"These executors sharing the same worker Id ${maxExecutor.workerId}," +
        s" ${executorsSharingSameWorker.mkString(",")}")

      // kill the worker and expect restarting all killed executors on other workers.
      val workerIdToKill = maxExecutor.workerId
      cluster.removeWorkerNode(hostName(workerIdToKill))

      val appMasterKilled = executorsSharingSameWorker
        .exists(_ == Constants.APPMASTER_DEFAULT_EXECUTOR_ID)

      def executorsMigrated(): Boolean = {
        val executors = restClient.queryExecutorBrief(appId)
        val newAppMaster = executors.find(_.executorId == Constants.APPMASTER_DEFAULT_EXECUTOR_ID)

        if (appMasterKilled) {
          newAppMaster.get.workerId != appMaster.get.workerId
        } else {
          // New executors will be started to replace killed executors.
          // The new executors will be assigned larger executor Id. We use this trick to detect
          // Whether new executors have been started successfully.
          executors.map(_.executorId).max > maxExecutor.executorId
        }
      }

      Util.retryUntil(() => {
        executorsMigrated()
      }, s"new executor created with id > ${maxExecutor.executorId} when worker is killed")

      // verify
      val laterAppMaster = restClient.queryStreamingAppDetail(appId)
      laterAppMaster.status shouldEqual ApplicationStatus.ACTIVE
      laterAppMaster.clock should be > 0L
    }
  }

  "kill master" should {
    "master will be down and all workers will attempt to reconnect and suicide after X seconds" in {
      // setup
      restartClusterRequired = true
      val masters = cluster.getMasterHosts
      val config = restClient.queryMasterConfig()
      val shutDownTimeout = Duration(config.getString("akka.cluster.auto-down-unreachable-after"))

      // exercise
      masters.foreach(cluster.removeMasterNode)
      info(s"will sleep ${shutDownTimeout.toSeconds}s and then check workers are down")
      Thread.sleep(shutDownTimeout.toMillis)

      // verify
      val aliveWorkers = cluster.getWorkerHosts
      Util.retryUntil(() => aliveWorkers.forall(worker => !cluster.nodeIsOnline(worker)),
        "all workers down")
    }
  }

  private def ensureClockStoredInMaster(): Unit = {
    // TODO: 5000ms is a fixed sync period in clock service.
    // we wait for 5000ms to assume the clock is stored
    Thread.sleep(5000)
  }
}