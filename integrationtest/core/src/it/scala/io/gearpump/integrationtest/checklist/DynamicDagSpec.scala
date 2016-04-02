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

import io.gearpump.integrationtest.{Util, TestSpecBase}
import io.gearpump.metrics.Metrics.Meter
import io.gearpump.streaming._
import io.gearpump.streaming.appmaster.ProcessorSummary

class DynamicDagSpec extends TestSpecBase {

  lazy val solJar = cluster.queryBuiltInExampleJars("sol-").head
  val splitTaskClass = "io.gearpump.streaming.examples.wordcount.Split"
  val sumTaskClass = "io.gearpump.streaming.examples.wordcount.Sum"
  val solName = "sol"

  "dynamic dag" should {
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

    "can replace down stream with wordcount's sum processor (new processor will have metrics)" in {
      // setup
      val appId = expectSolJarSubmittedWithAppId()

      // exercise
      val formerProcessors = restClient.queryStreamingAppDetail(appId).processors
      replaceProcessor(appId, 1, sumTaskClass)
      var laterProcessors: Map[ProcessorId, ProcessorSummary] = null
      Util.retryUntil(()=>{
        laterProcessors = restClient.queryStreamingAppDetail(appId).processors
        laterProcessors.size == formerProcessors.size + 1
      }, "new processor successfully added")
      processorHasThroughput(appId, laterProcessors.keySet.max, "receiveThroughput")
    }

    "can replace up stream with wordcount's split processor (new processor will have metrics)" in {
      // setup
      val appId = expectSolJarSubmittedWithAppId()

      // exercise
      val formerProcessors = restClient.queryStreamingAppDetail(appId).processors
      replaceProcessor(appId, 0, splitTaskClass)
      var laterProcessors: Map[ProcessorId, ProcessorSummary] = null
      Util.retryUntil(()=>{
        laterProcessors = restClient.queryStreamingAppDetail(appId).processors
        laterProcessors.size == formerProcessors.size + 1
      }, "new processor added")
      processorHasThroughput(appId, laterProcessors.keySet.max, "sendThroughput")
    }

    "fall back to last dag version when replacing a processor failid" in {
      // setup
      val appId = expectSolJarSubmittedWithAppId()

      // exercise
      val formerProcessors = restClient.queryStreamingAppDetail(appId).processors
      replaceProcessor(appId, 1, sumTaskClass)
      var laterProcessors: Map[ProcessorId, ProcessorSummary] = null
      Util.retryUntil(()=>{
        laterProcessors = restClient.queryStreamingAppDetail(appId).processors
        laterProcessors.size == formerProcessors.size + 1
      }, "new processor added")
      processorHasThroughput(appId, laterProcessors.keySet.max, "receiveThroughput")

      val fakeTaskClass = "io.gearpump.streaming.examples.wordcount.Fake"
      replaceProcessor(appId, laterProcessors.keySet.max, fakeTaskClass)
      Util.retryUntil(()=>{
        val processorsAfterFailure = restClient.queryStreamingAppDetail(appId).processors
        processorsAfterFailure.size == laterProcessors.size
      }, "new processor added")
      val currentClock = restClient.queryStreamingAppDetail(appId).clock
      Util.retryUntil(()=>restClient.queryStreamingAppDetail(appId).clock > currentClock,
        "app clock is advancing")
    }

    "fall back to last dag version when AppMaster HA triggered" in {
      // setup
      val appId = expectSolJarSubmittedWithAppId()

      // exercise
      val formerAppMaster = restClient.queryApp(appId).appMasterPath
      val formerProcessors = restClient.queryStreamingAppDetail(appId).processors
      replaceProcessor(appId, 1, sumTaskClass)
      var laterProcessors: Map[ProcessorId, ProcessorSummary] = null
      Util.retryUntil(()=>{
        laterProcessors = restClient.queryStreamingAppDetail(appId).processors
        laterProcessors.size == formerProcessors.size + 1
      }, "new processor added")
      processorHasThroughput(appId, laterProcessors.keySet.max, "receiveThroughput")

      restClient.killAppMaster(appId) shouldBe true
      Util.retryUntil(()=>restClient.queryApp(appId).appMasterPath != formerAppMaster,
        "new AppMaster created")
      val processors = restClient.queryStreamingAppDetail(appId).processors
      processors.size shouldEqual laterProcessors.size
    }

  }

  private def expectSolJarSubmittedWithAppId(): Int = {
    val appId = restClient.getNextAvailableAppId()
    val success = restClient.submitApp(solJar, cluster.getWorkerHosts.length)
    success shouldBe true
    expectAppIsRunning(appId, solName)
    Util.retryUntil(()=>restClient.queryStreamingAppDetail(appId).clock > 0, "app running")
    appId
  }

  private def replaceProcessor(
      appId: Int,
      formerProcessorId: Int,
      newTaskClass: String,
      newProcessorDescription: String = "",
      newParallelism: Int = 1): Unit = {
    val uploadedJar = restClient.uploadJar(wordCountJar)
    val replaceMe = new ProcessorDescription(formerProcessorId, newTaskClass,
      newParallelism, newProcessorDescription,
      jar = uploadedJar)

    // exercise
    val success = restClient.replaceStreamingAppProcessor(appId, replaceMe)
    success shouldBe true
  }

  private def processorHasThroughput(appId: Int, processorId: Int, metrics: String): Unit = {
    Util.retryUntil(()=>{
      val actual = restClient.queryStreamingAppMetrics(appId, current = false,
        path = "processor" + processorId)
      val throughput = actual.metrics.filter(_.value.name.endsWith(metrics))
      throughput.size should be > 0
      throughput.forall(_.value.asInstanceOf[Meter].count > 0L)
    }, "new processor has message received")
  }
  
}
