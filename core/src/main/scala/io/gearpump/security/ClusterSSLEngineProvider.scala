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

package io.gearpump.security

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.remote.transport.netty.SSLEngineProvider
import javax.net.ssl.SSLEngine

class ClusterSSLEngineProvider(system: ActorSystem) extends SSLEngineProvider {
  private val tls = ClusterTls.context(system.settings.config)
  override def createServerSSLEngine(): SSLEngine = ClusterTls.serverEngine(tls)
  override def createClientSSLEngine(): SSLEngine = {
    val engine = tls.createSSLEngine()
    engine.setUseClientMode(true)
    engine.setEnabledProtocols(Array("TLSv1.3", "TLSv1.2"))
    engine
  }
}
