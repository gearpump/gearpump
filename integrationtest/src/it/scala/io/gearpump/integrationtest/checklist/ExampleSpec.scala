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

import io.gearpump.integrationtest.{TestSpecBase, Util}
import io.gearpump.streaming._
import io.gearpump.streaming.appmaster.ProcessorSummary

/**
 * The test spec will perform destructive operations to check the stability
 */
class ExampleSpec extends TestSpecBase {

  "distributed shell" should {
    "todo" in {
    }
  }

  "dynamic dag" should {
    "can retrieve a list of built-in partitioner classes" in {
      val partitioners = restClient.queryBuiltInPartitioners()
      partitioners.length should be > 0
      partitioners.foreach(clazz =>
        clazz should startWith("io.gearpump.partitioner.")
      )
    }

    lazy val dynamicDagJar = cluster.queryBuiltInExampleJars("complexdag-").head
    lazy val dynamicDagName = "dag"

    "can change the parallelism and description of a processor" in {
      // setup
      val appId = restClient.submitApp(dynamicDagJar)
      expectAppIsRunning(appId, dynamicDagName)
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

    "can submit a dag as user application" in {

    }
  }

}
