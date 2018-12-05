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

package io.gearpump.util

import org.scalatest.{FlatSpec, Matchers}

class RestartPolicySpec extends FlatSpec with Matchers {

  "RestartPolicy" should "forbid too many restarts" in {
    val policy = new RestartPolicy(2)
    assert(policy.allowRestart)
    assert(policy.allowRestart)
    assert(!policy.allowRestart)
  }

  "RestartPolicy" should "forbid too many restarts in a window duration" in {
    val policy = new RestartPolicy(-1)
    assert(policy.allowRestart)
    assert(policy.allowRestart)
  }
}
