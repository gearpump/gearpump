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
package org.apache.gearpump.experiments.cluster.util

import org.apache.gearpump.experiments.cluster.ExecutorToAppMaster.ResponsesFromExecutor
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

class ResponseBuilderSpec extends WordSpec with Matchers with BeforeAndAfter {
  "ResponseBuilder" should {
    "aggregate ResponsesFromTasks" in {
      val executorId1 = 1
      val executorId2 = 2
      val responseBuilder = new ResponseBuilder
      val response1 = ResponsesFromExecutor(executorId1, List("task1", "task2"))
      val response2 = ResponsesFromExecutor(executorId2, List("task3", "task4"))
      val result = responseBuilder.aggregate(response1).aggregate(response2).toString()
      println(result)
      val expected = s"Execute results from executor $executorId1 : \ntask1task2\n" +
        s"Execute results from executor $executorId2 : \ntask3task4\n"
      assert(result == expected)
    }
  }
}
