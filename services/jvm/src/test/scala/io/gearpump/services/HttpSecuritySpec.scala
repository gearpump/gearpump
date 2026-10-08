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

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.gearpump.cluster.ClientToMaster.QueryMasterConfig
import io.gearpump.cluster.MasterToClient.MasterConfig
import io.gearpump.cluster.TestUtil
import io.gearpump.security.Authenticator
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.http.scaladsl.model.{FormData, Uri}
import org.apache.pekko.http.scaladsl.model.headers.{`Set-Cookie`, Cookie, HttpCookiePair}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import org.apache.pekko.testkit.TestActor.{AutoPilot, KeepRunning}
import org.apache.pekko.testkit.TestProbe
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

class HttpSecuritySpec extends AnyFlatSpec with Matchers with ScalatestRouteTest {
  override def testConfig: Config = {
    val enabled = TestUtil.UI_CONFIG.withValue(
      "gearpump.ui-security.authentication-enabled", ConfigValueFactory.fromAnyRef(true))
    enabled.withValue("gearpump.ui-security.config-file-based-authenticator.users.normal",
      ConfigValueFactory.fromAnyRef(TestUtil.UI_CONFIG.getString(
        "gearpump.ui-security.config-file-based-authenticator.admins.admin")))
  }

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

  it should "preserve path boundaries and slash handling when classifying permissions" in {
    Seq("terminate", "///terminate//", "/%74erminate/", "/api/v1.0/supervisor/",
      "/%61pi/v1%2E0/%73upervisor/addworker/1", "/api///v1.0//master///config//",
      "/api/v1.0/config", "/api/v1.0/master%2Fchild/config")
      .foreach { path =>
        assert(HttpAuthorization.requiredPermission(Uri.Path(path)) ==
          Authenticator.Admin.permissionLevel, path)
      }
    Seq("", "/", "/terminate/extra", "/termination", "/api/v1X0/master/config",
      "/api/v1.0/supervisors/addworker/1", "/other/api/v1.0/master/config",
      "/api/v1.0/master/configuration", "/api%2Fv1.0/master/config",
      "/api/v1.0/master%2Fconfig", "/api/v1.0/supervisor%2Faddworker/1",
      "/api/v1.0/master/%2563onfig")
      .foreach { path =>
        assert(HttpAuthorization.requiredPermission(Uri.Path(path)) ==
          Authenticator.Guest.permissionLevel, path)
      }
  }

  it should "render only the diagnostic allowlist, excluding arbitrary application secrets" in {
    val config = ConfigFactory.parseString("""
      gearpump.worker.slots = 4
      gearpump.services.http = 8090
      gearpump.ui-security.password = "hidden-password"
      application.custom-api-key = "hidden-key"
      application.safe-looking-value = "hidden-token"
    """)
    val rendered = SafeConfigRenderer.render(config, concise = true)
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
    import org.apache.pekko.http.scaladsl.server.AuthorizationFailedRejection
    implicit val timeout = RouteTestTimeout(20.seconds)
    val inner = new RouteService { override def route = complete("allowed") }
    val security = new SecurityService(inner, system)
    Seq(("normal", "admin"), ("guest", "guest"), ("admin", "admin")).foreach {
      case (user, password) =>
        var cookie: HttpCookiePair = null
        Post("/login", FormData("username" -> user, "password" -> password)) ~>
          security.route ~> check {
            val value = headers.collectFirst {
              case `Set-Cookie`(cookie) if cookie.name == "gearpump_token" => cookie
            }.get
            cookie = HttpCookiePair(value.name, value.value)
          }
        Seq(Post("/terminate"), Post("/api/v1.0/supervisor/addworker/1"),
          Get("/api/v1.0/master/config")).foreach { request =>
          request.addHeader(Cookie(cookie)) ~> security.route ~> check {
            if (user == "admin") assert(responseAs[String] == "allowed")
            else assert(rejection == AuthorizationFailedRejection)
          }
        }
    }
  }

  private def login(route: Route, user: String, password: String)
    (implicit timeout: RouteTestTimeout): HttpCookiePair = {
    var session: HttpCookiePair = null
    Post("/login", FormData("username" -> user, "password" -> password)) ~> route ~> check {
      assert(status.intValue() == 200)
      val cookie = headers.collectFirst {
        case `Set-Cookie`(value) if value.name == "gearpump_token" => value
      }.get
      session = HttpCookiePair(cookie.name, cookie.value)
    }
    session
  }

  it should "preserve forbidden responses through the complete REST and static routes" in {
    implicit val timeout = RouteTestTimeout(20.seconds)
    val master = TestProbe()(system)
    val route = Route.seal(new RestServices(master.ref, system).route)
    Seq(("normal", "admin"), ("guest", "guest")).foreach { case (user, password) =>
      val cookie = login(route, user, password)
      Seq(Post("/terminate"), Post("/api/v1.0/supervisor/addworker/1"),
        Get("/api/v1.0/master/config"), Get("/api/v1.0/master/%63onfig"),
        Get("/api/v1.0/worker/0/config"), Get("/api/v1.0/appmaster/1/executor/2/config"))
        .foreach { request =>
          request.addHeader(Cookie(cookie)) ~> route ~> check {
            assert(status.intValue() == 403)
          }
        }
    }
    master.expectNoMessage(100.millis)
  }

  it should "allow Admin diagnostics through the complete REST route" in {
    implicit val timeout = RouteTestTimeout(20.seconds)
    val master = TestProbe()(system)
    master.setAutoPilot(new AutoPilot {
      override def run(sender: ActorRef, message: Any): AutoPilot = {
        message match {
          case QueryMasterConfig => sender ! MasterConfig(testConfig)
        }
        KeepRunning
      }
    })
    val route = Route.seal(new RestServices(master.ref, system).route)
    val cookie = login(route, "admin", "admin")
    Get("/api/v1.0/master/config").addHeader(Cookie(cookie)) ~> route ~> check {
      assert(status.intValue() == 200)
      assert(!responseAs[String].contains("pbkdf2"))
    }
    master.expectMsg(QueryMasterConfig)
  }

  it should "retain unauthenticated responses and public assets in the complete REST route" in {
    implicit val timeout = RouteTestTimeout(20.seconds)
    val master = TestProbe()(system)
    val route = Route.seal(new RestServices(master.ref, system).route)
    Seq(Post("/terminate"), Get("/api/v1.0/master/config")).foreach { request =>
      request ~> route ~> check {
        assert(status.intValue() == 401)
      }
    }
    Seq("/", "/login/login.html", "/login/login.js").foreach { path =>
      Get(path) ~> route ~> check {
        assert(status.intValue() == 200)
      }
    }
    master.expectNoMessage(100.millis)
  }
}
