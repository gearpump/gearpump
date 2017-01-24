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

import scala.concurrent.duration._
import org.apache.gearpump.cluster.{ApplicationStatus, MasterToAppMaster}
import org.apache.gearpump.cluster.master.MasterStatus
import org.apache.gearpump.cluster.worker.{WorkerId, WorkerSummary}
import org.apache.gearpump.integrationtest.{TestSpecBase, Util}

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
      val appId = restClient.getNextAvailableAppId()
      val success = restClient.submitApp(wordCountJar, cluster.getWorkerHosts.length)
      success shouldBe true
      expectAppIsRunning(appId, wordCountName)
      restClient.listRunningApps().length shouldEqual 1
    }
  }

  "submit application (wordcount)" should {
    "find a running application after submission" in {
      // exercise
      val appId = restClient.getNextAvailableAppId()
      val success = restClient.submitApp(wordCountJar, cluster.getWorkerHosts.length)
      success shouldBe true
      expectAppIsRunning(appId, wordCountName)
    }

    "reject a repeated submission request while the application is running" in {
      // setup
      val appId = restClient.getNextAvailableAppId()
      val formerSubmissionSuccess = restClient.submitApp(wordCountJar,
        cluster.getWorkerHosts.length)
      formerSubmissionSuccess shouldBe true
      expectAppIsRunning(appId, wordCountName)

      // exercise
      val success = restClient.submitApp(wordCountJar, cluster.getWorkerHosts.length)
      success shouldBe false
    }

    "reject an invalid submission (the jar file path is incorrect)" in {
      // exercise
      val success = restClient.submitApp(wordCountJar + ".missing", cluster.getWorkerHosts.length)
      success shouldBe false
    }

    "submit a wordcount application with 4 split and 3 sum processors and expect " +
      "parallelism of processors match the given number" in {
      // setup
      val splitNum = 4
      val sumNum = 3
      val appId = restClient.getNextAvailableAppId()

      // exercise
      val success = restClient.submitApp(wordCountJar, cluster.getWorkerHosts.length,
        s"$wordCountClass -split $splitNum -sum $sumNum")
      success shouldBe true
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
      val appId = restClient.getNextAvailableAppId()
      val success = restClient.submitApp(wordCountJar, cluster.getWorkerHosts.length)
      success shouldBe true
      expectAppIsRunning(appId, wordCountName)

      // exercise
      expectMetricsAvailable(
        restClient.queryStreamingAppMetrics(appId, current = true).metrics.nonEmpty,
        "metrics available")
      val actual = restClient.queryStreamingAppMetrics(appId, current = true)
      actual.path shouldEqual s"app$appId.processor*"
      actual.metrics.foreach(metric => {
        metric.time should be > 0L
        metric.value should not be null
      })
      val formerMetricsDump = actual.metrics.toString()

      expectMetricsAvailable({
        val laterMetrics = restClient.queryStreamingAppMetrics(appId, current = true).metrics
        laterMetrics.nonEmpty && laterMetrics.toString() != formerMetricsDump
      }, "metrics available")
    }

    "can obtain application corresponding executors' metrics and " +
      "the metrics will keep changing" in {
      // setup
      val appId = restClient.getNextAvailableAppId()
      val success = restClient.submitApp(wordCountJar, cluster.getWorkerHosts.length)
      success shouldBe true
      expectAppIsRunning(appId, wordCountName)

      // exercise
      expectMetricsAvailable(
        restClient.queryExecutorMetrics(appId, current = true).metrics.nonEmpty,
        "metrics available")
      val actual = restClient.queryExecutorMetrics(appId, current = true)
      actual.path shouldEqual s"app$appId.executor*"
      actual.metrics.foreach(metric => {
        metric.time should be > 0L
        metric.value should not be null
      })
      val formerMetricsDump = actual.metrics.toString()

      expectMetricsAvailable({
        val laterMetrics = restClient.queryExecutorMetrics(appId, current = true).metrics
        laterMetrics.nonEmpty && laterMetrics.toString() != formerMetricsDump
      }, "metrics available")
    }
  }

  "kill application" should {
    "a running application should be killed" in {
      // setup
      val appId = restClient.getNextAvailableAppId()
      val success = restClient.submitApp(wordCountJar, cluster.getWorkerHosts.length)
      success shouldBe true
      expectAppIsRunning(appId, wordCountName)

      // exercise
      killAppAndVerify(appId)
    }

    "fail when attempting to kill a stopped application" in {
      // setup
      val appId = restClient.getNextAvailableAppId()
      val submissionSucess = restClient.submitApp(wordCountJar, cluster.getWorkerHosts.length)
      submissionSucess shouldBe true
      expectAppIsRunning(appId, wordCountName)
      killAppAndVerify(appId)

      // exercise
      val success = restClient.killApp(appId)
      success shouldBe false
    }

    "fail when attempting to kill a non-exist application" in {
      // setup
      val freeAppId = restClient.listApps().length + 1

      // exercise
      val success = restClient.killApp(freeAppId)
      success shouldBe false
    }
  }

  "cluster information" should {
    "retrieve 1 master for a non-HA cluster" in {
      // exercise
      val masterSummary = restClient.queryMaster()
      masterSummary.cluster.map(_.toTuple) shouldEqual cluster.getMastersAddresses
      masterSummary.aliveFor should be > 0L
      masterSummary.masterStatus shouldEqual MasterStatus.Synced
    }

    "retrieve the same number of workers as cluster has" in {
      // setup
      val expectedWorkersCount = cluster.getWorkerHosts.size

      // exercise
      var runningWorkers: Array[WorkerSummary] = Array.empty
      Util.retryUntil(() => {
        runningWorkers = restClient.listRunningWorkers()
        runningWorkers.length == expectedWorkersCount
      }, "all workers running")
      runningWorkers.foreach { worker =>
        worker.state shouldEqual "active"
      }
    }

    "find a newly added worker instance" in {
      // setup
      restartClusterRequired = true
      val formerWorkersCount = cluster.getWorkerHosts.length
      Util.retryUntil(() => restClient.listRunningWorkers().length == formerWorkersCount,
        "all workers running")
      val workerName = "newWorker"

      // exercise
      cluster.addWorkerNode(workerName)
      Util.retryUntil(() => restClient.listRunningWorkers().length > formerWorkersCount,
        "new worker added")
      cluster.getWorkerHosts.length shouldEqual formerWorkersCount + 1
      restClient.listRunningWorkers().length shouldEqual formerWorkersCount + 1
    }

    "retrieve 0 worker, if cluster is started without any workers" in {
      // setup
      restartClusterRequired = true
      cluster.shutDown()

      // exercise
      cluster.start(workerNum = 0)
      cluster.getWorkerHosts.length shouldEqual 0
      restClient.listRunningWorkers().length shouldEqual 0
    }

    "can obtain master's metrics and the metrics will keep changing" in {
      // exercise
      expectMetricsAvailable(
        restClient.queryMasterMetrics(current = true).metrics.nonEmpty, "metrics available")
      val actual = restClient.queryMasterMetrics(current = true)
      actual.path shouldEqual s"master"
      actual.metrics.foreach(metric => {
        metric.time should be > 0L
        metric.value should not be null
      })
      val formerMetricsDump = actual.metrics.toString()

      expectMetricsAvailable({
        val laterMetrics = restClient.queryMasterMetrics(current = true).metrics
        laterMetrics.nonEmpty && laterMetrics.toString() != formerMetricsDump
      }, "metrics available")
    }

    "can obtain workers' metrics and the metrics will keep changing" in {
      // exercise
      restClient.listRunningWorkers().foreach { worker =>
        val workerId = worker.workerId
        expectMetricsAvailable(
          restClient.queryWorkerMetrics(workerId, current = true).metrics.nonEmpty,
          "metrics available")
        val actual = restClient.queryWorkerMetrics(workerId, current = true)
        actual.path shouldEqual s"worker${WorkerId.render(workerId)}"
        actual.metrics.foreach(metric => {
          metric.time should be > 0L
          metric.value should not be null
        })
        val formerMetricsDump = actual.metrics.toString()

        expectMetricsAvailable({
          val laterMetrics = restClient.queryWorkerMetrics(workerId, current = true).metrics
          laterMetrics.nonEmpty && laterMetrics.toString() != formerMetricsDump
        }, "metrics available")
      }
    }
  }

  "configuration" should {
    "retrieve the configuration of master and match particular values" in {
      // exercise
      val actual = restClient.queryMasterConfig()
      actual.hasPath("gearpump") shouldBe true
      actual.hasPath("gearpump.cluster") shouldBe true
      actual.getString("gearpump.hostname") shouldEqual cluster.getMasterHosts.mkString(",")
    }

    "retrieve the configuration of worker X and match particular values" in {
      // exercise
      restClient.listRunningWorkers().foreach { worker =>
        val actual = restClient.queryWorkerConfig(worker.workerId)
        actual.hasPath("gearpump") shouldBe true
        actual.hasPath("gearpump.worker") shouldBe true
      }
    }

    "retrieve the configuration of executor X and match particular values" in {
      // setup
      val appId = restClient.getNextAvailableAppId()

      // exercise
      val success = restClient.submitApp(wordCountJar, cluster.getWorkerHosts.length)
      success shouldBe true
      restClient.queryExecutorBrief(appId).foreach { executor =>
        val executorId = executor.executorId
        val actual = restClient.queryExecutorConfig(appId, executorId)
        actual.hasPath("gearpump") shouldBe true
        actual.hasPath("gearpump.executor") shouldBe true
        actual.getInt("gearpump.applicationId") shouldEqual appId
        actual.getInt("gearpump.executorId") shouldEqual executorId
      }
    }

    "retrieve the configuration of application X and match particular values" in {
      // setup
      val appId = restClient.getNextAvailableAppId()

      // exercise
      val success = restClient.submitApp(wordCountJar, cluster.getWorkerHosts.length)
      success shouldBe true
      val actual = restClient.queryAppMasterConfig(appId)
      actual.hasPath("gearpump") shouldBe true
      actual.hasPath("gearpump.appmaster") shouldBe true
    }
  }

  "application life-cycle" should {
    "newly started application should be configured same as the previous one, after restart" in {
      // setup
      val originSplitNum = 4
      val originSumNum = 3
      val originAppId = restClient.getNextAvailableAppId()
      val success = restClient.submitApp(wordCountJar, cluster.getWorkerHosts.length,
        s"$wordCountClass -split $originSplitNum -sum $originSumNum")
      success shouldBe true
      expectAppIsRunning(originAppId, wordCountName)
      val originAppDetail = restClient.queryStreamingAppDetail(originAppId)

      // exercise
      Util.retryUntil(() => restClient.restartApp(originAppId), "app restarted")
      val killedApp = restClient.queryApp(originAppId)
      killedApp.appId shouldEqual originAppId
      killedApp.status shouldEqual ApplicationStatus.TERMINATED
      val newAppId = originAppId + 1
      expectAppIsRunning(newAppId, wordCountName)
      val runningApps = restClient.listRunningApps()
      runningApps.length shouldEqual 1
      val newAppDetail = restClient.queryStreamingAppDetail(runningApps.head.appId)
      newAppDetail.appName shouldEqual originAppDetail.appName
      newAppDetail.processors.size shouldEqual originAppDetail.processors.size
      newAppDetail.processors.get(0).get.parallelism shouldEqual originSplitNum
      newAppDetail.processors.get(1).get.parallelism shouldEqual originSumNum
    }
  }

  private def killAppAndVerify(appId: Int): Unit = {
    val success = restClient.killApp(appId)
    success shouldBe true

    val actualApp = restClient.queryApp(appId)
    actualApp.appId shouldEqual appId
    actualApp.status shouldEqual ApplicationStatus.TERMINATED
  }

  private def expectMetricsAvailable(condition: => Boolean, conditionDescription: String): Unit = {
    val config = restClient.queryMasterConfig()
    val reportInterval = Duration(config.getString("gearpump.metrics.report-interval-ms") + "ms")
    Util.retryUntil(() => condition, conditionDescription, interval = reportInterval)
  }
}
