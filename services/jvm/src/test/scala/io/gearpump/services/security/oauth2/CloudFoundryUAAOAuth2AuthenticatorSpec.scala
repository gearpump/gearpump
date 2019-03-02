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

package io.gearpump.services.security.oauth2

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.HttpEntity.Strict
import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.ConfigFactory
import io.gearpump.security.Authenticator
import io.gearpump.services.security.oauth2.impl.CloudFoundryUAAOAuth2Authenticator
import org.scalatest.FlatSpec
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

class CloudFoundryUAAOAuth2AuthenticatorSpec extends FlatSpec with ScalatestRouteTest {

  implicit val actorSystem: ActorSystem = system
  private val server = new MockOAuth2Server(system, null)
  server.start()
  private val serverHost = s"http://127.0.0.1:${server.port}"

  val configMap = Map(
    "class" ->
      "io.gearpump.services.security.oauth2.impl.CloudFoundryUAAOAuth2Authenticator",
    "callback" -> s"$serverHost/login/oauth2/cloudfoundryuaa/callback",
    "clientid" -> "gearpump_test2",
    "clientsecret" -> "gearpump_test2",
    "default-userrole" -> "user",
    "icon" -> "/icons/uaa.png",
    "uaahost" -> serverHost,
    "additional-authenticator-enabled" -> "false")

  val configString = ConfigFactory.parseMap(configMap.asJava)

  lazy val uaa = {
    val uaa = new CloudFoundryUAAOAuth2Authenticator
    uaa.init(configString, system.dispatcher)
    uaa
  }

  it should "generate the correct authorization request" in {
    val parameters = Uri(uaa.getAuthorizationUrl).query().toMap
    assert(parameters("response_type") == "code")
    assert(parameters("client_id") == configMap("clientid"))
    assert(parameters("redirect_uri") == configMap("callback"))
    assert(parameters("scope") == "openid,cloud_controller.read")
  }

  it should "authenticate the authorization code and return the correct profile" in {
    val code = Map("code" -> "QGGVeA")
    val accessToken = "e2922002-0218-4513-a62d-1da2ba64ee4c"
    val refreshToken = "eyJhbGciOiJSUzI1NiJ9.eyJqdGkiOiI2Nm"
    val mail = "test@gearpump.apache.org"

    def accessTokenEndpoint(request: HttpRequest): HttpResponse = {
      assert(request.getHeader("Authorization").get.value() ==
        "Basic Z2VhcnB1bXBfdGVzdDI6Z2VhcnB1bXBfdGVzdDI=")
      assert(request.entity.contentType.mediaType.value == "application/x-www-form-urlencoded")

      val body = request.entity.asInstanceOf[Strict].data.decodeString("UTF-8")
      val form = Uri./.withQuery(Query(body)).query().toMap

      assert(form("grant_type") == "authorization_code")
      assert(form("code") == "QGGVeA")
      assert(form("response_type") == "token")
      assert(form("redirect_uri") == configMap("callback"))

      val response =
        s"""
        |{
        |  "access_token": "$accessToken",
        |  "token_type": "bearer",
        |  "refresh_token": "$refreshToken",
        |  "expires_in": 43199,
        |  "scope": "openid",
        |  "jti": "e8739474-b2fa-42eb-a9ad-e065bf79d7e9"
        |}
        """.stripMargin
      HttpResponse(entity = HttpEntity(ContentType(`application/json`), response))
    }

    def protectedResourceEndpoint(request: HttpRequest): HttpResponse = {
      assert(request.getUri().query().get("access_token").get == accessToken)
      val response =
        s"""
        |{
        |    "user_id": "e2922002-0218-4513-a62d-1da2ba64ee4c",
        |    "user_name": "user",
        |    "email": "$mail"
        |}
        """.stripMargin
      HttpResponse(entity = HttpEntity(ContentType(`application/json`), response))
    }

    server.requestHandler = (request: HttpRequest) => {
      if (request.uri.path.startsWith(Path("/oauth/token"))) {
        accessTokenEndpoint(request)
      } else if (request.uri.path.startsWith(Path("/userinfo"))) {
        protectedResourceEndpoint(request)
      } else {
        fail("Unexpected access to " + request.uri.toString())
      }
    }

    val userFuture = uaa.authenticate(code)
    val user = Await.result(userFuture, 30.seconds)
    assert(user.user == mail)
    assert(user.permissionLevel == Authenticator.User.permissionLevel)
  }

  override def cleanUp(): Unit = {
    server.stop()
    uaa.close()
    super.cleanUp()
  }
}
