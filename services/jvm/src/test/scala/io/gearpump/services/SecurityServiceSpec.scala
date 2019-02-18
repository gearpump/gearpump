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

import akka.actor.ActorSystem
import akka.http.scaladsl.model.FormData
import akka.http.scaladsl.model.headers.{`Set-Cookie`, Cookie, _}
import akka.http.scaladsl.server.{AuthorizationFailedRejection, _}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.typesafe.config.Config
import io.gearpump.cluster.TestUtil
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import scala.concurrent.duration._
// NOTE: This cannot be removed!!!

class SecurityServiceSpec
  extends FlatSpec with ScalatestRouteTest with Matchers with BeforeAndAfterAll {

  override def testConfig: Config = TestUtil.UI_CONFIG

  implicit def actorSystem: ActorSystem = system

  it should "return 401 if not authenticated" in {
    val security = new SecurityService(SecurityServiceSpec.resource, actorSystem)

    implicit val customTimeout = RouteTestTimeout(15.seconds)

    (Get(s"/resource") ~> security.route) ~> check {
      assert(rejection.isInstanceOf[AuthenticationFailedRejection])
    }
  }

  "guest" should "get protected resource after authentication" in {
    val security = new SecurityService(SecurityServiceSpec.resource, actorSystem)

    implicit val customTimeout = RouteTestTimeout(15.seconds)

    var cookie: HttpCookiePair = null
    (Post(s"/login", FormData("username" -> "guest", "password" -> "guest"))
      ~> security.route) ~> check {
      assert("{\"user\":\"guest\"}" == responseAs[String])
      assert(status.intValue() == 200)
      assert(header[`Set-Cookie`].isDefined)
      val httpCookie = header[`Set-Cookie`].get.cookie
      assert(httpCookie.name == "gearpump_token")
      cookie = HttpCookiePair.apply(httpCookie.name, httpCookie.value)
    }

    // After authentication, everything is fine.
    Get("/resource").addHeader(Cookie(cookie)) ~> security.route ~> check {
      responseAs[String] shouldEqual "OK"
    }

    // However, guest cannot access high-permission operations, like POST.
    Post("/resource").addHeader(Cookie(cookie)) ~> security.route ~> check {
      assert(rejection == AuthorizationFailedRejection)
    }

    // Logout, should clear the session
    Post(s"/logout").addHeader(Cookie(cookie)) ~> security.route ~> check {
      assert("{\"user\":\"guest\"}" == responseAs[String])
      assert(status.intValue() == 200)
      assert(header[`Set-Cookie`].isDefined)
      val httpCookie = header[`Set-Cookie`].get.cookie
      assert(httpCookie.name == "gearpump_token")
      assert(httpCookie.value == "deleted")
    }

    // Access again, rejected this time.
    Get("/resource") ~> security.route ~> check {
      assert(rejection.isInstanceOf[AuthenticationFailedRejection])
    }

    Post("/resource") ~> security.route ~> check {
      assert(rejection.isInstanceOf[AuthenticationFailedRejection])
    }
  }

  "admin" should "get protected resource after authentication" in {
    val security = new SecurityService(SecurityServiceSpec.resource, actorSystem)

    implicit val customTimeout = RouteTestTimeout(15.seconds)

    var cookie: HttpCookiePair = null
    (Post(s"/login", FormData("username" -> "admin", "password" -> "admin"))
      ~> security.route) ~> check {
      assert("{\"user\":\"admin\"}" == responseAs[String])
      assert(status.intValue() == 200)
      assert(header[`Set-Cookie`].isDefined)
      val httpCookie = header[`Set-Cookie`].get.cookie
      assert(httpCookie.name == "gearpump_token")
      cookie = HttpCookiePair(httpCookie.name, httpCookie.value)
    }

    // After authentication, everything is fine.
    Get("/resource").addHeader(Cookie(cookie)) ~> security.route ~> check {
      responseAs[String] shouldEqual "OK"
    }

    // Not like guest, admimn can also access POST
    Post("/resource").addHeader(Cookie(cookie)) ~> security.route ~> check {
      responseAs[String] shouldEqual "OK"
    }

    // Logout, should clear the session
    Post(s"/logout").addHeader(Cookie(cookie)) ~> security.route ~> check {
      assert("{\"user\":\"admin\"}" == responseAs[String])
      assert(status.intValue() == 200)
      assert(header[`Set-Cookie`].isDefined)
      val httpCookie = header[`Set-Cookie`].get.cookie
      assert(httpCookie.name == "gearpump_token")
      assert(httpCookie.value == "deleted")
    }

    // Access again, rejected this time.
    Get("/resource") ~> security.route ~> check {
      assert(rejection.isInstanceOf[AuthenticationFailedRejection])
    }

    Post("/resource") ~> security.route ~> check {
      assert(rejection.isInstanceOf[AuthenticationFailedRejection])
    }
  }
}

object SecurityServiceSpec {

  val resource = new RouteService {
    override def route: Route = {
      get {
        path("resource") {
          complete("OK")
        }
      } ~
      post {
        path("resource") {
          complete("OK")
        }
      }
    }
  }
}
