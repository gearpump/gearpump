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
import io.gearpump.services.security.oauth2.GoogleOAuth2AuthenticatorSpec.MockGoogleAuthenticator
import io.gearpump.services.security.oauth2.impl.GoogleOAuth2Authenticator
import org.scalatest.FlatSpec
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

class GoogleOAuth2AuthenticatorSpec extends FlatSpec with ScalatestRouteTest {

  implicit val actorSystem: ActorSystem = system
  private val server = new MockOAuth2Server(system, null)
  server.start()
  private val serverHost = s"http://127.0.0.1:${server.port}"

  val configMap = Map(
    "class" -> "io.gearpump.services.security.oauth2.impl.GoogleOAuth2Authenticator",
    "callback" -> s"$serverHost/login/oauth2/google/callback",
    "clientid" -> "170234147043-a1tag68jtq6ab4bi11jvsj7vbaqcmhkt.apps.googleusercontent.com",
    "clientsecret" -> "ioeWLLDipz2S7aTDXym2-obe",
    "default-userrole" -> "guest",
    "icon" -> "/icons/google.png")

  val configString = ConfigFactory.parseMap(configMap.asJava)

  private lazy val google = {
    val google = new MockGoogleAuthenticator(serverHost)
    google.init(configString, system.dispatcher)
    google
  }

  it should "generate the correct authorization request" in {
    val parameters = Uri(google.getAuthorizationUrl).query().toMap
    assert(parameters("response_type") == "code")
    assert(parameters("client_id") == configMap("clientid"))
    assert(parameters("redirect_uri") == configMap("callback"))
    assert(parameters("scope") == GoogleOAuth2Authenticator.Scope)
  }

  it should "authenticate the authorization code and return the correct profile" in {
    val code = Map("code" -> "4/PME0pfxjiBA42SukR-OTGl7fpFzTWzvZPf1TbkpXL4M#")
    val accessToken = "e2922002-0218-4513-a62d-1da2ba64ee4c"
    val mail = "test@gearpump.apache.org"

    def accessTokenEndpoint(request: HttpRequest): HttpResponse = {

      assert(request.entity.contentType.mediaType.value == "application/x-www-form-urlencoded")

      val body = request.entity.asInstanceOf[Strict].data.decodeString("UTF-8")
      val form = Uri./.withQuery(Query(body)).query().toMap

      assert(form("client_id") == configMap("clientid"))
      assert(form("client_secret") == configMap("clientsecret"))
      assert(form("grant_type") == "authorization_code")
      assert(form("code") == code("code"))
      assert(form("redirect_uri") == configMap("callback"))
      assert(form("scope") == GoogleOAuth2Authenticator.Scope)

      // scalastyle:off line.size.limit
      val response =
        s"""
        |{
        | "access_token": "$accessToken",
        | "token_type": "Bearer",
        | "expires_in": 3591,
        | "id_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImY1NjQyYzY2MzdhYWQyOTJiOThlOGIwN2MwMzIxN2QwMzBmOTdkODkifQ.eyJpc3"
        |}
        """.stripMargin
      // scalastyle:on line.size.limit

      HttpResponse(entity = HttpEntity(ContentType(`application/json`), response))
    }

    def protectedResourceEndpoint(request: HttpRequest): HttpResponse = {
      assert(request.getUri().query().get("access_token").get == accessToken)
      val response =
        s"""
        |{
        |   "kind": "plus#person",
        |   "etag": "4OZ_Kt6ujOh1jaML_U6RM6APqoE/mZ57HcMOYXaNXYXS5XEGJ9yVsI8",
        |   "nickname": "gearpump",
        |   "gender": "female",
        |   "emails": [
        |     {
        |       "value": "$mail",
        |       "type": "account"
        |     }
        |   ]
        | }
        """.stripMargin
      HttpResponse(entity = HttpEntity(ContentType(`application/json`), response))
    }

    server.requestHandler = (request: HttpRequest) => {
      if (request.uri.path.startsWith(Path("/oauth2/v4/token"))) {
        accessTokenEndpoint(request)
      } else if (request.uri.path.startsWith(Path("/plus/v1/people/me"))) {
        protectedResourceEndpoint(request)
      } else {
        fail("Unexpected access to " + request.uri.toString())
      }
    }

    val userFuture = google.authenticate(code)
    val user = Await.result(userFuture, 30.seconds)
    assert(user.user == mail)
    assert(user.permissionLevel == Authenticator.Guest.permissionLevel)
  }

  override def cleanUp(): Unit = {
    server.stop()
    google.close()
    super.cleanUp()
  }
}

object GoogleOAuth2AuthenticatorSpec {
  class MockGoogleAuthenticator(host: String) extends GoogleOAuth2Authenticator {
    protected override def authorizeUrl: String = {
      super.authorizeUrl.replace("https://accounts.google.com", host)
    }

    protected override def accessTokenEndpoint: String = {
      super.accessTokenEndpoint.replace("https://www.googleapis.com", host)
    }

    protected override def protectedResourceUrl: String = {
      super.protectedResourceUrl.replace("https://www.googleapis.com", host)
    }
  }
}