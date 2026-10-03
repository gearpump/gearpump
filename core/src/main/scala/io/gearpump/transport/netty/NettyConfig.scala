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

package io.gearpump.transport.netty

import com.typesafe.config.Config
import io.gearpump.util.Constants

class NettyConfig(conf: Config) {

  val applicationCapability = io.gearpump.security.ControlCapability.token(conf,
    io.gearpump.security.ControlCapability.AppKey)
  require(applicationCapability.matches("[A-Za-z0-9_-]{43}"),
    "Task transport requires an AppMaster-issued application capability")
  lazy val tls = io.gearpump.security.ClusterTls.context(conf)
  val buffer_size = conf.getInt(Constants.NETTY_BUFFER_SIZE)
  val max_retries = conf.getInt(Constants.NETTY_MAX_RETRIES)
  val base_sleep_ms = conf.getInt(Constants.NETTY_BASE_SLEEP_MS)
  val max_sleep_ms = conf.getInt(Constants.NETTY_MAX_SLEEP_MS)
  val messageBatchSize = conf.getInt(Constants.NETTY_MESSAGE_BATCH_SIZE)
  require(messageBatchSize > 0 && messageBatchSize <= AuthenticatedFrames.MaxBatchLength / 2,
    "Task batch setting exceeds authenticated transport bounds")
  val flushCheckInterval = conf.getInt(Constants.NETTY_FLUSH_CHECK_INTERVAL)

  def newTransportSerializer: ITransportMessageSerializer = {
    Class
      .forName(conf.getString(Constants.GEARPUMP_TRANSPORT_SERIALIZER))
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[ITransportMessageSerializer]
  }
}
