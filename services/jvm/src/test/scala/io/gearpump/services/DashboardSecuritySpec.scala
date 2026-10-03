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
import io.gearpump.services.security.{DashboardDeployment, OAuthState}
import org.apache.pekko.http.scaladsl.model.FormData
import org.apache.pekko.http.scaladsl.model.headers.{Cookie, HttpCookiePair, RawHeader, `Set-Cookie`}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

class DashboardSecuritySpec extends AnyFlatSpec with Matchers with ScalatestRouteTest {
  override def testConfig: Config = ConfigFactory.parseString("""
    gearpump.ui-security.oauth2-authenticator-enabled = true
    gearpump.ui-security.oauth2-authenticators.test {
      class = "io.gearpump.services.TestStateOAuthAuthenticator"
      icon = "/icons/test.png"
    }
  """).withFallback(TestUtil.UI_CONFIG)
  implicit val routeTimeout: RouteTestTimeout = RouteTestTimeout(20.seconds)
  it should "reject password login without a valid header/cookie CSRF pair" in {
    val service = new SecurityService(new RouteService {
      override def route = complete("mutated")
    }, system)
    val request = Post("/login", FormData("username" -> "admin", "password" -> "admin"))
    request ~> service.route ~> check { assert(!handled) }
    var token: HttpCookiePair = null
    Get("/login/csrf") ~> service.route ~> check {
      val cookie = headers.collect { case s: `Set-Cookie`
        if s.cookie.name == "__Host-XSRF-TOKEN" => s.cookie }.head
      assert(cookie.secure && !cookie.httpOnly && cookie.path.contains("/"))
      token = HttpCookiePair(cookie.name, cookie.value)
    }
    request.addHeader(Cookie(token)) ~> service.route ~> check { assert(!handled) }
    request.addHeader(Cookie(token)).addHeader(RawHeader("X-XSRF-TOKEN", "forged")) ~>
      service.route ~> check { assert(!handled) }
    request.addHeader(Cookie(token)).addHeader(RawHeader("X-XSRF-TOKEN", token.value)) ~>
      service.route ~> check {
        assert(status.intValue() == 200)
        val cookie = headers.collect { case s: `Set-Cookie`
          if s.cookie.name == "gearpump_token" => s.cookie }.head
        assert(cookie.secure && cookie.httpOnly)
      }
  }
  it should "validate browser state before calling an OAuth provider and reject replay" in {
    val service = new SecurityService(new RouteService {
      override def route = complete("resource")
    }, system)
    TestStateOAuthAuthenticator.calls.set(0)
    var browser: HttpCookiePair = null
    var state: String = null
    Get("/login/oauth2/test/authorize") ~> service.route ~> check {
      state = header[org.apache.pekko.http.scaladsl.model.headers.Location].get.uri.query().get("state").get
      val cookie = headers.collect { case value: `Set-Cookie`
        if value.cookie.name == "__Host-gearpump_oauth" => value.cookie }.head
      assert(cookie.secure && cookie.httpOnly && cookie.maxAge.contains(300L))
      browser = HttpCookiePair(cookie.name, cookie.value)
    }
    val callback = Get(s"/login/oauth2/test/callback?code=code&state=$state")
    callback ~> service.route ~> check { assert(!handled) }
    Get("/login/oauth2/test/callback?code=code&state=wrong").addHeader(Cookie(browser)) ~>
      service.route ~> check { assert(!handled) }
    assert(TestStateOAuthAuthenticator.calls.get() == 0)
    callback.addHeader(Cookie(browser)) ~> service.route ~> check {
      assert(status.intValue() == 307)
      assert(headers.exists { case value: `Set-Cookie` =>
        value.cookie.name == "gearpump_token"; case _ => false })
    }
    callback.addHeader(Cookie(browser)) ~> service.route ~> check { assert(!handled) }
    assert(TestStateOAuthAuthenticator.calls.get() == 1)
  }
  it should "bind OAuth state to the browser/provider and consume it only once" in {
    var time = 0L
    val states = new OAuthState(() => time, ttlNanos = 100, capacity = 2)
    val first = states.begin("google").get
    val second = states.begin("google").get
    assert(first.state != second.state && first.browser != second.browser)
    assert(states.begin("google").isEmpty)
    assert(!states.consume(first.state, second.browser, "google"))
    assert(!states.consume(first.state, first.browser, "other"))
    assert(!states.consume("", first.browser, "google"))
    assert(states.consume(first.state, first.browser, "google"))
    assert(!states.consume(first.state, first.browser, "google"))
    time = 100
    assert(!states.consume(second.state, second.browser, "google"))
    assert(states.begin("google").isDefined)
  }
  it should "fail closed on disabled authentication, cleartext and unsafe proxy listeners" in {
    val config = ConfigFactory.parseString("""
      gearpump.ui-security.authentication-enabled = true
      gearpump.services {
        host = "127.0.0.1"
        https-enabled = true
        tls-termination-proxy = false
        public-origin = "https://localhost:8090"
      }
    """).withFallback(TestUtil.UI_CONFIG)
    DashboardDeployment.validate(config)
    Seq("gearpump.ui-security.authentication-enabled=false",
      "gearpump.services.https-enabled=false",
      "pekko.http.session.cookie.secure=false",
      "gearpump.services.public-origin=\"http://localhost:8090\"")
      .foreach { overrideConfig =>
        intercept[IllegalArgumentException] {
          DashboardDeployment.validate(ConfigFactory.parseString(overrideConfig).withFallback(config))
        }
      }
    val proxy = ConfigFactory.parseString("""
      gearpump.services.https-enabled=false
      gearpump.services.tls-termination-proxy=true
    """).withFallback(config)
    DashboardDeployment.validate(proxy)
    intercept[IllegalArgumentException] {
      DashboardDeployment.validate(ConfigFactory.parseString(
        "gearpump.services.host=\"0.0.0.0\"").withFallback(proxy))
    }
  }
}

object TestStateOAuthAuthenticator {
  val calls = new java.util.concurrent.atomic.AtomicInteger()
}
class TestStateOAuthAuthenticator extends
    io.gearpump.services.security.oauth2.OAuth2Authenticator {
  override def init(config: Config, ec: scala.concurrent.ExecutionContext): Unit = ()
  override def getAuthorizationUrl(state: String): String =
    "https://identity.example/authorize?state=" + state
  override def authenticate(parameters: Map[String, String]) = {
    TestStateOAuthAuthenticator.calls.incrementAndGet()
    scala.concurrent.Future.successful(SecurityService.UserSession("oauth-user", 1))
  }
  override def close(): Unit = ()
}
