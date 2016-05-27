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

import org.apache.log4j.Logger

import org.apache.gearpump.integrationtest.{Docker, TestSpecBase, Util}
import org.apache.gearpump.streaming._
import org.apache.gearpump.streaming.appmaster.ProcessorSummary

/**
 * The test spec will perform destructive operations to check the stability
 */
class ExampleSpec extends TestSpecBase {

  private val LOG = Logger.getLogger(getClass)

  "distributed shell" should {
    "execute commands on machines where its executors are running" in {
      val distShellJar = cluster.queryBuiltInExampleJars("distributedshell-").head
      val mainClass = "org.apache.gearpump.examples.distributedshell.DistributedShell"
      val clientClass = "org.apache.gearpump.examples.distributedshell.DistributedShellClient"
      val appId = restClient.getNextAvailableAppId()
      val success = restClient.submitApp(distShellJar, cluster.getWorkerHosts.length, mainClass)
      success shouldBe true
      expectAppIsRunning(appId, "DistributedShell")
      val args = Array(
        clientClass,
        "-appid", appId.toString,
        "-command", "hostname"
      )

      val expectedHostNames = cluster.getWorkerHosts.map(Docker.getHostName(_))

      def verify(): Boolean = {
        val workerNum = cluster.getWorkerHosts.length
        val result = commandLineClient.submitAppAndCaptureOutput(distShellJar,
          workerNum, args.mkString(" ")).split("\n").
          filterNot(line => line.startsWith("[INFO]") || line.isEmpty)
        expectedHostNames.forall(result.contains)
      }

      Util.retryUntil(() => verify(),
        s"executors started on all expected hosts ${expectedHostNames.mkString(", ")}")
    }
  }

  "wordcount" should {
    val wordCountJarNamePrefix = "wordcount-"
    behave like streamingApplication(wordCountJarNamePrefix, wordCountName)

    "can submit immediately after killing a former one" in {
      // setup
      val formerAppId = restClient.getNextAvailableAppId()
      val formerSubmissionSuccess =
        restClient.submitApp(wordCountJar, cluster.getWorkerHosts.length)
      formerSubmissionSuccess shouldBe true
      expectAppIsRunning(formerAppId, wordCountName)
      Util.retryUntil(() =>
        restClient.queryStreamingAppDetail(formerAppId).clock > 0, "app running")
      restClient.killApp(formerAppId)

      // exercise
      val appId = formerAppId + 1
      val success = restClient.submitApp(wordCountJar, cluster.getWorkerHosts.length)
      success shouldBe true
      expectAppIsRunning(appId, wordCountName)
    }
  }

  "wordcount(java)" should {
    val wordCountJavaJarNamePrefix = "wordcountjava-"
    val wordCountJavaName = "wordcountJava"
    behave like streamingApplication(wordCountJavaJarNamePrefix, wordCountJavaName)
  }

  "sol" should {
    val solJarNamePrefix = "sol-"
    val solName = "sol"
    behave like streamingApplication(solJarNamePrefix, solName)
  }

  "complexdag" should {
    val dynamicDagJarNamePrefix = "complexdag-"
    val dynamicDagName = "dag"
    behave like streamingApplication(dynamicDagJarNamePrefix, dynamicDagName)
  }

  def streamingApplication(jarNamePrefix: String, appName: String): Unit = {
    lazy val jar = cluster.queryBuiltInExampleJars(jarNamePrefix).head

    "can obtain application clock and the clock will keep changing" in {
      // setup
      val appId = restClient.getNextAvailableAppId()
      val success = restClient.submitApp(jar, cluster.getWorkerHosts.length)
      success shouldBe true
      expectAppIsRunning(appId, appName)

      // exercise
      Util.retryUntil(() => restClient.queryStreamingAppDetail(appId).clock > 0, "app submitted")
      val formerClock = restClient.queryStreamingAppDetail(appId).clock
      Util.retryUntil(() => restClient.queryStreamingAppDetail(appId).clock > formerClock,
        "app clock is advancing")
    }

    "can change the parallelism and description of a processor" in {
      // setup
      val appId = restClient.getNextAvailableAppId()
      val formerSubmissionSuccess = restClient.submitApp(jar, cluster.getWorkerHosts.length)
      formerSubmissionSuccess shouldBe true
      expectAppIsRunning(appId, appName)
      val formerProcessors = restClient.queryStreamingAppDetail(appId).processors
      val processor0 = formerProcessors.get(0).get
      val expectedProcessorId = formerProcessors.size
      val expectedParallelism = processor0.parallelism + 1
      val expectedDescription = processor0.description + "new"
      val replaceMe = new ProcessorDescription(processor0.id, processor0.taskClass,
        expectedParallelism, description = expectedDescription)

      // exercise
      val success = restClient.replaceStreamingAppProcessor(appId, replaceMe)
      success shouldBe true
      var laterProcessors: Map[ProcessorId, ProcessorSummary] = null
      Util.retryUntil(() => {
        laterProcessors = restClient.queryStreamingAppDetail(appId).processors
        laterProcessors.size == formerProcessors.size + 1
      }, "new process added")
      val laterProcessor0 = laterProcessors.get(expectedProcessorId).get
      laterProcessor0.parallelism shouldEqual expectedParallelism
      laterProcessor0.description shouldEqual expectedDescription
    }
  }
}
