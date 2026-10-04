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

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.gearpump.cluster.TestUtil
import org.apache.pekko.actor.ActorSystem
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

class ConfigFileBasedAuthenticatorSpec extends AnyFlatSpec with Matchers {
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

  it should "reject configured sample and legacy credentials at startup" in {
    val config = TestUtil.UI_CONFIG.withValue(
      "gearpump.ui-security.config-file-based-authenticator.admins.admin",
      com.typesafe.config.ConfigValueFactory.fromAnyRef(
        "AeGxGOxlU8QENdOXejCeLxy+isrCv0TrS37HwA=="))
    intercept[IllegalArgumentException] {
      new ConfigFileBasedAuthenticator(config)
    }
  }

  it should "start with shipped empty role maps and deny the former default accounts" in {
    val defaults = ConfigFactory.parseResources("geardefault.conf").resolve()
    val config = defaults.getConfig("gearpump-ui").withFallback(defaults)
    val root = "gearpump.ui-security.config-file-based-authenticator"
    Seq("admins", "users", "guests").foreach { role =>
      assert(config.getConfig(root + "." + role).isEmpty)
    }
    val authenticator = new ConfigFileBasedAuthenticator(config)
    Seq("admin", "guest").foreach { user =>
      Await.result(authenticator.authenticate(user, user, ExecutionContext.global), 5.seconds)
        .shouldBe(Authenticator.UnAuthenticated)
    }
  }

  it should "validate every role entry even when another role contains the same username" in {
    val root = "gearpump.ui-security.config-file-based-authenticator"
    val valid = TestUtil.UI_CONFIG.getString(root + ".admins.admin")
    val legacy = "AeGxGOxlU8QENdOXejCeLxy+isrCv0TrS37HwA=="
    Seq("admins", "users", "guests").foreach { invalidRole =>
      val roles = Seq("admins", "users", "guests").foldLeft(TestUtil.UI_CONFIG) { (conf, role) =>
        conf.withValue(root + "." + role, ConfigValueFactory.fromMap(
          java.util.Collections.singletonMap("duplicate",
            if (role == invalidRole) legacy else valid)))
      }
      intercept[IllegalArgumentException] { new ConfigFileBasedAuthenticator(roles) }
    }
  }

  it should "verify passwords without scheduling work on the caller execution context" in {
    val forbidden = new ExecutionContext {
      override def execute(work: Runnable): Unit = {
        throw new AssertionError("Password verification used the HTTP executor")
      }
      override def reportFailure(error: Throwable): Unit = throw new AssertionError(error)
    }
    val authenticator = new ConfigFileBasedAuthenticator(TestUtil.UI_CONFIG)
    Await.result(authenticator.authenticate("admin", "admin", forbidden), 10.seconds)
      .shouldBe(Authenticator.Admin)
    Await.result(authenticator.authenticate("admin", "wrong", forbidden), 10.seconds)
      .shouldBe(Authenticator.UnAuthenticated)
  }

}
