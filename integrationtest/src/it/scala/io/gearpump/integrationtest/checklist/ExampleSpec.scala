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

import io.gearpump.integrationtest.{Docker, TestSpecBase, Util}
import io.gearpump.metrics.Metrics.Meter
import io.gearpump.streaming._
import io.gearpump.streaming.appmaster.ProcessorSummary

/**
 * The test spec will perform destructive operations to check the stability
 */
class ExampleSpec extends TestSpecBase {

  "distributed shell" should {
    "execute commands on machines where its executors are running" in {
      val distShellJar = cluster.queryBuiltInExampleJars("distributedshell-").head
      val mainClass = "io.gearpump.examples.distributedshell.DistributedShell"
      val clientClass = "io.gearpump.examples.distributedshell.DistributedShellClient"
      val appId = restClient.submitApp(distShellJar, mainClass)
      expectAppIsRunning(appId, "DistributedShell")
      val args = Array(
        clientClass,
        "-appid", appId.toString,
        "-command", "hostname"
      )
      val expectedHostNames = cluster.getWorkerHosts.map(Docker.execAndCaptureOutput(_, "hostname"))

      def verify(): Boolean = {
        val result = commandLineClient.submitAppAndCaptureOutput(distShellJar, args.mkString(" ")).split("\n").
          filterNot(line => line.startsWith("[INFO]") || line.isEmpty)
        expectedHostNames.forall(result.contains)
      }

      Util.retryUntil(verify())
    }
  }

  "wordcount" should {
    val wordCountJarNamePrefix = "wordcount-"
    behave like streamingApplication(wordCountJarNamePrefix, wordCountName)

    "can submit immediately after killing a former one" in {
      // setup
      val formerAppId = restClient.submitApp(wordCountJar)
      expectAppIsRunning(formerAppId, wordCountName)
      Util.retryUntil(restClient.queryStreamingAppDetail(formerAppId).clock > 0)
      restClient.killApp(formerAppId)

      // exercise
      val appId = restClient.submitApp(wordCountJar)
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

    "can replace sum processor with wordcount's sum processor (new processor will have metrics)" in {
      // setup
      val jar = cluster.queryBuiltInExampleJars(solJarNamePrefix).head
      val appId = restClient.submitApp(jar)
      expectAppIsRunning(appId, solName)
      val formerProcessors = restClient.queryStreamingAppDetail(appId).processors
      val formerSumProcessor = formerProcessors.get(1).get
      val uploadedJar = restClient.uploadJar(wordCountJar)
      val expectedProcessorId = formerProcessors.size
      val expectedProcessorDescription = "replaced processor description"
      val newTaskClass = "io.gearpump.streaming.examples.wordcount.Sum"
      val replaceMe = new ProcessorDescription(formerSumProcessor.id, newTaskClass,
        formerSumProcessor.parallelism, expectedProcessorDescription,
        jar = uploadedJar)

      // exercise
      val success = restClient.replaceStreamingAppProcessor(appId, replaceMe)
      success shouldBe true
      var laterProcessors: Map[ProcessorId, ProcessorSummary] = null
      Util.retryUntil({
        laterProcessors = restClient.queryStreamingAppDetail(appId).processors
        laterProcessors.size == formerProcessors.size + 1
      })

      // verify
      val laterSumProcessor = laterProcessors.get(expectedProcessorId).get
      laterSumProcessor.parallelism shouldEqual formerSumProcessor.parallelism
      laterSumProcessor.description shouldEqual expectedProcessorDescription
      laterSumProcessor.taskClass shouldEqual newTaskClass
      Util.retryUntil({
        val actual = restClient.queryStreamingAppMetrics(appId, current = false,
          path = "processor" + expectedProcessorId)
        val throughput = actual.metrics.filter(_.value.name.endsWith("receiveThroughput"))
        throughput.size should be > 0
        val hasThroughput = throughput.forall(_.value.asInstanceOf[Meter].count > 0L)
        hasThroughput
      })
    }
  }

  "dynamic dag" should {
    val dynamicDagJarNamePrefix = "complexdag-"
    val dynamicDagName = "dag"
    behave like streamingApplication(dynamicDagJarNamePrefix, dynamicDagName)

    "can retrieve a list of built-in partitioner classes" in {
      val partitioners = restClient.queryBuiltInPartitioners()
      partitioners.length should be > 0
      partitioners.foreach(clazz =>
        clazz should startWith("io.gearpump.partitioner.")
      )
    }

    "can compose a wordcount application from scratch" in {
      // todo: blocked by #1450
    }
  }

  def streamingApplication(jarNamePrefix: String, appName: String): Unit = {
    lazy val jar = cluster.queryBuiltInExampleJars(jarNamePrefix).head

    "can obtain application clock and the clock will keep changing" in {
      // setup
      val appId = restClient.submitApp(jar)
      expectAppIsRunning(appId, appName)

      // exercise
      Util.retryUntil(restClient.queryStreamingAppDetail(appId).clock > 0)
      val formerClock = restClient.queryStreamingAppDetail(appId).clock
      Util.retryUntil(restClient.queryStreamingAppDetail(appId).clock > formerClock)
    }

    "can change the parallelism and description of a processor" in {
      // setup
      val appId = restClient.submitApp(jar)
      expectAppIsRunning(appId, appName)
      val formerProcessors = restClient.queryStreamingAppDetail(appId).processors
      val processor0 = formerProcessors.get(0).get
      val expectedProcessorId = formerProcessors.size
      val expectedParallelism = processor0.parallelism + 1
      val expectedDescription = processor0.description + "new"
      val replaceMe = new ProcessorDescription(processor0.id, processor0.taskClass, expectedParallelism,
        description = expectedDescription)

      // exercise
      val success = restClient.replaceStreamingAppProcessor(appId, replaceMe)
      success shouldBe true
      var laterProcessors: Map[ProcessorId, ProcessorSummary] = null
      Util.retryUntil({
        laterProcessors = restClient.queryStreamingAppDetail(appId).processors
        laterProcessors.size == formerProcessors.size + 1
      })
      val laterProcessor0 = laterProcessors.get(expectedProcessorId).get
      laterProcessor0.parallelism shouldEqual expectedParallelism
      laterProcessor0.description shouldEqual expectedDescription
    }
  }

}
