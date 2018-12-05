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

/**
 * When one executor or task fails, Gearpump will try to start. However, if it fails after
 * multiple retries, then we abort.
 *
 * @param totalNrOfRetries The total number of times is allowed to be restarted, negative value
 *                         means no limit, if the limit is exceeded the policy will not allow
 *                         to restart
 */
class RestartPolicy(totalNrOfRetries: Int) {
  private var historicalRetries: Int = 0

  def allowRestart: Boolean = {
    historicalRetries += 1
    totalNrOfRetries < 0 || historicalRetries <= totalNrOfRetries
  }
}
