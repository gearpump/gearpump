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

import scala.concurrent.duration
import scala.concurrent.duration._
import io.gearpump.cluster.master.MasterStatus

/**
 * The test spec checks REST service usage
 */
class RestServiceSpec extends TestSpecBase {

  "query system version" should {
    "retrieve the current version number" in {
      restClient.queryVersion() should not be empty
    }
  }

  "list applications" should {
    "retrieve 0 application after cluster just started" in {
      restClient.listRunningApps().length shouldEqual 0
    }

    "retrieve 1 application after the first application submission" in {
      // exercise
      val appId = restClient.submitApp(wordCountJar)
      expectAppIsRunning(appId, wordCountName)
      restClient.listRunningApps().length shouldEqual 1
    }
  }

  "submit application (wordcount)" should {
    "find a running application after submission" in {
      // exercise
      val appId = restClient.submitApp(wordCountJar)
      expectAppIsRunning(appId, wordCountName)
    }

    "reject a repeated submission request while the application is running" in {
      // setup
      var appId = restClient.submitApp(wordCountJar)
      expectAppIsRunning(appId, wordCountName)

      // exercise
      appId = restClient.submitApp(wordCountJar)
      appId shouldEqual -1
    }

    "reject an invalid submission (the jar file path is incorrect)" in {
      // exercise
      val appId = restClient.submitApp(wordCountJar + ".missing")
      appId shouldEqual -1
    }

    "submit a wordcount application with 4 split and 3 sum processors and expect parallelism of processors match the given number" in {
      // setup
      val splitNum = 4
      val sumNum = 3

      // exercise
      val appId = restClient.submitApp(wordCountJar, s"-split $splitNum -sum $sumNum")
      expectAppIsRunning(appId, wordCountName)
      val processors = restClient.queryStreamingAppDetail(appId).processors
      processors.size shouldEqual 2
      val splitProcessor = processors.get(0).get
      splitProcessor.parallelism shouldEqual splitNum
      val sumProcessor = processors.get(1).get
      sumProcessor.parallelism shouldEqual sumNum
    }

    "can obtain application metrics and the metrics will keep changing" in {
      // setup
      val appId = restClient.submitApp(wordCountJar)
      expectAppIsRunning(appId, wordCountName)

      // exercise
      Util.retryUntil(
        restClient.queryStreamingAppMetrics(appId, current = true).metrics.nonEmpty,
        timeout = duration.Duration(5, MINUTES))
      val actual = restClient.queryStreamingAppMetrics(appId, current = true)
      actual.path shouldEqual s"app$appId.processor*"
      assert(actual.metrics.head.time > 0)
      val formerMetricsDump = actual.metrics.toString()

      Util.retryUntil({
        val laterMetrics = restClient.queryStreamingAppMetrics(appId, current = true).metrics
        laterMetrics.nonEmpty && laterMetrics.toString() != formerMetricsDump
      }, timeout = duration.Duration(5, MINUTES))
    }

    "can obtain application corresponding executors' metrics and the metrics will keep changing" in {
      // setup
      val appId = restClient.submitApp(wordCountJar)
      expectAppIsRunning(appId, wordCountName)

      // exercise
      Util.retryUntil(
        restClient.queryExecutorMetrics(appId, current = true).metrics.nonEmpty,
        timeout = duration.Duration(5, MINUTES))
      val actual = restClient.queryExecutorMetrics(appId, current = true)
      actual.path shouldEqual s"app$appId.executor*"
      assert(actual.metrics.head.time > 0)
      val formerMetricsDump = actual.metrics.toString()

      Util.retryUntil({
        val laterMetrics = restClient.queryExecutorMetrics(appId, current = true).metrics
        laterMetrics.nonEmpty && laterMetrics.toString() != formerMetricsDump
      }, timeout = duration.Duration(5, MINUTES))
    }
  }

  "kill application" should {
    "a running application should be killed" in {
      // setup
      val appId = restClient.submitApp(wordCountJar)
      expectAppIsRunning(appId, wordCountName)

      // exercise
      killAppAndVerify(appId)
    }

    "should fail when attempting to kill a stopped application" in {
      // setup
      val appId = restClient.submitApp(wordCountJar)
      expectAppIsRunning(appId, wordCountName)
      killAppAndVerify(appId)

      // exercise
      val success = restClient.killApp(appId)
      success shouldBe false
    }

    "should fail when attempting to kill a non-exist application" in {
      // setup
      val freeAppId = restClient.listApps().length + 1

      // exercise
      val success = restClient.killApp(freeAppId)
      success shouldBe false
    }
  }

  "cluster information" should {
    "retrieve 1 master for a non-HA cluster" in {
      // setup
      val expectedMastersCount = cluster.getMasterHosts.size
      val expectedMaster = cluster.getMasters.head

      // exercise
      val masters = restClient.listMasters()
      val masterSummary = restClient.queryMaster()
      masters.size shouldEqual expectedMastersCount
      masters.head shouldEqual expectedMaster
      masterSummary.aliveFor should be > 0L
      masterSummary.masterStatus shouldEqual MasterStatus.Synced
    }

    "retrieve the same number of workers as cluster has" in {
      // setup
      val expectedWorkersCount = cluster.getWorkerHosts.size

      // exercise
      val runningWorkers = restClient.listRunningWorkers()
      runningWorkers.size shouldEqual expectedWorkersCount
      runningWorkers.foreach { worker =>
        worker.state shouldEqual MasterToAppMaster.AppMasterActive
      }
    }

    "find a newly added worker instance" in {
      // setup
      val oldWorkersCount = restClient.listRunningWorkers().size
      val workerName = "newWorker"

      // exercise
      try {
        cluster.newWorkerNode(workerName)
        Util.retryUntil(restClient.listRunningWorkers().size > oldWorkersCount)
        restClient.listRunningWorkers().size shouldEqual (oldWorkersCount + 1)
      } finally {
        cluster.removeWorkerNode(workerName)
        Util.retryUntil(restClient.listRunningWorkers().size == oldWorkersCount)
      }
    }

    "retrieve 0 worker, if cluster is started without any workers" in {
      // setup
      val originWorkersCount = cluster.getWorkerHosts.size

      // exercise
      try {
        restClient.listRunningWorkers().size shouldEqual originWorkersCount
        while (cluster.getWorkerHosts.nonEmpty) {
          cluster.removeWorkerNode(cluster.getWorkerHosts.head)
          val workersCount = cluster.getWorkerHosts.size
          Util.retryUntil(restClient.listRunningWorkers().size == workersCount)
          restClient.listRunningWorkers().size shouldEqual workersCount
        }
        restClient.listRunningWorkers().size shouldEqual 0
      } finally {
        (0 until originWorkersCount).foreach { index =>
          cluster.newWorkerNode(s"worker$index")
        }
        Util.retryUntil(restClient.listRunningWorkers().size == originWorkersCount)
      }
    }

    "can obtain master's metrics and the metrics will keep changing" in {
      // exercise
      Util.retryUntil(
        restClient.queryMasterMetrics(current = true).metrics.nonEmpty,
        timeout = duration.Duration(5, MINUTES))
      val actual = restClient.queryMasterMetrics(current = true)
      actual.path shouldEqual s"master"
      assert(actual.metrics.head.time > 0)
      val formerMetricsDump = actual.metrics.toString()

      Util.retryUntil({
        val laterMetrics = restClient.queryMasterMetrics(current = true).metrics
        laterMetrics.nonEmpty && laterMetrics.toString() != formerMetricsDump
      }, timeout = duration.Duration(5, MINUTES))
    }

    "can obtain workers' metrics and the metrics will keep changing" in {
    }
  }

  "configuration" should {
    "retrieve the configuration of master and match particular values" in {
      // exercise
      val actual = restClient.queryMasterConfig()
      actual.hasPath("gearpump") shouldBe true
      actual.hasPath("gearpump.cluster") shouldBe true
      actual.getString("gearpump.hostname") shouldEqual cluster.getMasterHosts.head
    }

    "retrieve the configuration of worker X and match particular values" in {

    }

    "retrieve the configuration of executor X and match particular values" in {

    }

    "retrieve the configuration of application X and match particular values" in {

    }
  }

  "application life-cycle" should {
    "newly started application should be configured same as the previous one, after restart" in {

    }
  }

  private def killAppAndVerify(appId: Int): Unit = {
    val success = restClient.killApp(appId)
    success shouldBe true

    val actualApp = restClient.queryApp(appId)
    actualApp.appId shouldEqual appId
    actualApp.status shouldEqual MasterToAppMaster.AppMasterInActive
  }

}