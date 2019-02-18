/*
 * Licensed under the Apache License, Version 2.0 (the
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
package io.gearpump.examples.distributedshell

import io.gearpump.examples.distributedshell.DistShellAppMaster.{ShellCommandResult, ShellCommandResultAggregator}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

class ShellCommandResultAggregatorSpec extends WordSpec with Matchers with BeforeAndAfter {
  "ShellCommandResultAggregator" should {
    "aggregate ShellCommandResult" in {
      val executorId1 = 1
      val executorId2 = 2
      val responseBuilder = new ShellCommandResultAggregator
      val response1 = ShellCommandResult(executorId1, "task1")
      val response2 = ShellCommandResult(executorId2, "task2")
      val result = responseBuilder.aggregate(response1).aggregate(response2).toString()
      val expected = s"Execute results from executor $executorId1 : \ntask1\n" +
        s"Execute results from executor $executorId2 : \ntask2\n"
      assert(result == expected)
    }
  }
}
