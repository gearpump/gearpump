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

package io.gearpump.streaming.state.impl

object PersistentStateConfig {

  val STATE_CHECKPOINT_ENABLE = "state.checkpoint.enable"
  val STATE_CHECKPOINT_INTERVAL_MS = "state.checkpoint.interval.ms"
  val STATE_CHECKPOINT_STORE_FACTORY = "state.checkpoint.store.factory"
  val STATE_WINDOW_SIZE = "state.window.size"
  val STATE_WINDOW_STEP = "state.window.step"
}
