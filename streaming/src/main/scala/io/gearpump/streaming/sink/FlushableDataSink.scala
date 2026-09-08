/*
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.streaming.sink

import scala.concurrent.duration.FiniteDuration

/** A data sink that wants its task to invoke `flush` periodically on the task actor thread. */
trait FlushableDataSink extends DataSink {

  /** How often the sink should be offered an opportunity to flush buffered data. */
  def flushInterval: FiniteDuration

  /** Flushes buffered data. This method is invoked on the same actor thread as `write`. */
  def flush(): Unit

  /** Whether task watermarks must wait for a successful flush. */
  def flushOnWatermark: Boolean = true
}
