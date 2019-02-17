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

import akka.actor.ActorSystem
import io.gearpump.cluster.TestUtil
import org.scalatest.{FlatSpec, Matchers}
import scala.concurrent.Await
import scala.concurrent.duration._

class ConfigFileBasedAuthenticatorSpec extends FlatSpec with Matchers {
  it should "authenticate correctly" in {
    val config = TestUtil.UI_CONFIG
    implicit val system = ActorSystem("ConfigFileBasedAuthenticatorSpec", config)
    implicit val ec = system.dispatcher
    val timeout = 30.seconds

    val authenticator = new ConfigFileBasedAuthenticator(config)
    val guest = Await.result(authenticator.authenticate("guest", "guest", ec), timeout)
    val admin = Await.result(authenticator.authenticate("admin", "admin", ec), timeout)

    val nonexist = Await.result(authenticator.authenticate("nonexist", "nonexist", ec), timeout)

    val failedGuest = Await.result(authenticator.authenticate("guest", "wrong", ec), timeout)
    val failedAdmin = Await.result(authenticator.authenticate("admin", "wrong", ec), timeout)

    assert(guest == Authenticator.Guest)
    assert(admin == Authenticator.Admin)
    assert(nonexist == Authenticator.UnAuthenticated)
    assert(failedGuest == Authenticator.UnAuthenticated)
    assert(failedAdmin == Authenticator.UnAuthenticated)

    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }
}
