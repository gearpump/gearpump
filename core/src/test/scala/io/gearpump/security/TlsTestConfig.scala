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

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

/** Public test-only key material; never use this certificate outside tests. */
object TlsTestConfig {
  private val path = java.nio.file.Paths.get(
    getClass.getResource("/security-test-only.p12").toURI).toString
  val config: Config = ConfigFactory.parseString("""
    gearpump.security.tls.store-type = "PKCS12"
    gearpump.security.tls.password = "gearpump-test-only"
    gearpump.jarstore.access-token = "test-only-credential-012345678901234567890123456789"
  """).withValue("gearpump.security.tls.key-store", ConfigValueFactory.fromAnyRef(path))
    .withValue("gearpump.security.tls.trust-store", ConfigValueFactory.fromAnyRef(path))
}
