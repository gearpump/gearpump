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

package io.gearpump.services

import com.typesafe.config.{Config, ConfigFactory}
import io.gearpump.cluster.ClusterConfig

/** Deliberately small public diagnostic schema; never render arbitrary application config. */
private[services] object SafeConfigRenderer {
  private val allowed = Set("gearpump.hostname", "gearpump.worker.slots",
    "gearpump.services.host", "gearpump.services.http",
    "gearpump.transport.max-retries", "gearpump.transport.message-batch-size")

  def render(config: Config, concise: Boolean): String = {
    Option(config).fold("{}") { value =>
      val safe = allowed.foldLeft(ConfigFactory.empty()) { (result, path) =>
        if (value.hasPath(path)) result.withValue(path, value.getValue(path)) else result
      }
      ClusterConfig.render(safe, concise)
    }
  }
}
