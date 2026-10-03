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

import com.typesafe.config.Config
import java.nio.file.{Files, Paths}
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, SSLEngine, TrustManagerFactory}

/** Explicit trust/key material; no insecure or cleartext fallback. */
object ClusterTls {
  def context(config: Config): SSLContext = {
    val tls = config.getConfig("gearpump.security.tls")
    val password = tls.getString("password").toCharArray
    def load(path: String): KeyStore = {
      require(path.nonEmpty, "Cluster TLS key-store and trust-store are required")
      val store = KeyStore.getInstance(tls.getString("store-type"))
      val in = Files.newInputStream(Paths.get(path))
      try store.load(in, password) finally in.close()
      store
    }
    try {
      val keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      keys.init(load(tls.getString("key-store")), password)
      val trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      trust.init(load(tls.getString("trust-store")))
      val context = SSLContext.getInstance("TLS")
      context.init(keys.getKeyManagers, trust.getTrustManagers, new SecureRandom())
      context
    } finally java.util.Arrays.fill(password, '\u0000')
  }
  def serverEngine(context: SSLContext): SSLEngine = {
    val engine = context.createSSLEngine()
    engine.setUseClientMode(false)
    engine.setNeedClientAuth(true)
    engine.setEnabledProtocols(Array("TLSv1.3", "TLSv1.2"))
    engine
  }
}
