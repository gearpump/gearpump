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
import io.gearpump.cluster.TestUtil
import io.gearpump.security.Authenticator
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HttpSecuritySpec extends AnyFlatSpec with Matchers with ScalatestRouteTest {
  private var csrfToken: org.apache.pekko.http.scaladsl.model.headers.HttpCookiePair = null
  private def csrf(security: SecurityService): Unit = {
    Get("/login/csrf") ~> security.route ~> check {
      val cookie = headers.collect {
        case value: org.apache.pekko.http.scaladsl.model.headers.`Set-Cookie`
            if value.cookie.name == "__Host-XSRF-TOKEN" => value.cookie
      }.head
      csrfToken = org.apache.pekko.http.scaladsl.model.headers.HttpCookiePair(
        cookie.name, cookie.value)
    }
  }
  private def protectedRequest(request: org.apache.pekko.http.scaladsl.model.HttpRequest,
      session: Option[org.apache.pekko.http.scaladsl.model.headers.HttpCookiePair] = None) = {
    request.addHeader(org.apache.pekko.http.scaladsl.model.headers.Cookie(
      (session.toSeq :+ csrfToken).toList)).addHeader(
      org.apache.pekko.http.scaladsl.model.headers.RawHeader("X-XSRF-TOKEN", csrfToken.value))
  }

  override def testConfig: Config = TestUtil.UI_CONFIG.withValue(
    "gearpump.ui-security.config-file-based-authenticator.users.normal",
    com.typesafe.config.ConfigValueFactory.fromAnyRef(TestUtil.UI_CONFIG.getString(
      "gearpump.ui-security.config-file-based-authenticator.admins.admin")))

  it should "require Admin for every privileged route including encoded segments" in {
    Seq("/terminate", "/api/v1.0/master/config", "/api/v1.0/master/%63onfig",
      "/api/v1.0/worker/0/config", "/api/v1.0/appmaster/1/executor/2/config",
      "/api/v1.0/supervisor/addworker/1", "/api/v1.0/supervisor/removeworker/0")
      .foreach { path =>
        assert(HttpAuthorization.requiredPermission(Uri(path).path) ==
          Authenticator.Admin.permissionLevel)
      }
    assert(HttpAuthorization.requiredPermission(Uri("/api/v1.0/master/applist").path) ==
      Authenticator.Guest.permissionLevel)
  }

  it should "render only the diagnostic allowlist, excluding arbitrary application secrets" in {
    val config = ConfigFactory.parseString("""
      gearpump.worker.slots = 4
      gearpump.services.http = 8090
      gearpump.ui-security.password = "hidden-password"
      application.custom-api-key = "hidden-key"
      application.safe-looking-value = "hidden-token"
    """)
    val rendered = ConfigDiagnostics.render(config, concise = true)
    assert(rendered.contains("8090"))
    assert(!rendered.contains("hidden"))
    assert(!rendered.contains("application"))
  }

  it should "return bounded errors without exception details or stack frames" in {
    val route = handleExceptions(RestServices.exceptionHandler) {
      get { throw new IllegalArgumentException("SECRET /private/path") }
    }
    Get("/") ~> route ~> check {
      assert(status.intValue() == 500)
      val body = responseAs[String]
      assert(body.startsWith("Internal server error; errorId="))
      assert(body.length < 100)
      assert(!body.contains("SECRET"))
      assert(!body.contains("IllegalArgumentException"))
    }
  }

  it should "deny User and Guest sessions before privileged routes execute" in {
    import org.apache.pekko.http.scaladsl.model.FormData
    import org.apache.pekko.http.scaladsl.model.headers.{Cookie, HttpCookiePair, `Set-Cookie`}
    import org.apache.pekko.http.scaladsl.server.AuthorizationFailedRejection
    import org.apache.pekko.http.scaladsl.testkit.RouteTestTimeout
    import scala.concurrent.duration._
    implicit val timeout = RouteTestTimeout(20.seconds)
    val inner = new RouteService { override def route = complete("allowed") }
    val security = new SecurityService(inner, system)
    csrf(security)
    Seq(("normal", "admin"), ("guest", "guest"), ("admin", "admin")).foreach {
      case (user, password) =>
        var cookie: HttpCookiePair = null
        protectedRequest(Post("/login", FormData("username" -> user, "password" -> password))) ~>
          security.route ~> check {
            val value = header[`Set-Cookie`].get.cookie
            cookie = HttpCookiePair(value.name, value.value)
          }
        Seq(Post("/terminate"), Post("/api/v1.0/supervisor/addworker/1"),
          Get("/api/v1.0/master/config")).foreach { request =>
          protectedRequest(request, Some(cookie)) ~> security.route ~> check {
            if (user == "admin") assert(responseAs[String] == "allowed")
            else assert(rejection == AuthorizationFailedRejection)
          }
        }
    }
  }
}
