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

package io.gearpump.services.main

import com.softwaremill.pekkohttpsession.SessionConfig
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.gearpump.services.SecurityService
import java.util.Base64
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ServicesSpec extends AnyFlatSpec with Matchers {
  it should "generate independent 64-byte session keys" in {
    val first = Services.randomSeverSecret()
    val second = Services.randomSeverSecret()
    assert(Base64.getDecoder.decode(first).length == 64)
    assert(Base64.getDecoder.decode(second).length == 64)
    assert(first != second)
  }

  it should "load the generated secret and session settings from the resolved Pekko library" in {
    val secret = Services.randomSeverSecret()
    val config = ConfigFactory.parseResources("geardefault.conf")
      .withValue(SecurityService.SESSION_MANAGER_KEY, ConfigValueFactory.fromAnyRef(secret))
      .withFallback(ConfigFactory.defaultReference()).resolve()
    assert(!config.hasPath("akka.http.session"))
    val session = SessionConfig.fromConfig(config)
    session.serverSecret shouldBe secret
    session.sessionCookieConfig.name shouldBe "gearpump_token"
    session.sessionMaxAgeSeconds shouldBe Some(604800L)
    session.sessionEncryptData shouldBe true
  }

}
