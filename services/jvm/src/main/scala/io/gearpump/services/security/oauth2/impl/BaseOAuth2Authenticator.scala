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

package io.gearpump.services.security.oauth2.impl

import com.github.scribejava.core.builder.ServiceBuilder
import com.github.scribejava.core.builder.api.DefaultApi20
import com.github.scribejava.core.oauth.AccessTokenRequestParams
import com.github.scribejava.core.model._
import com.github.scribejava.core.oauth.OAuth20Service
import com.github.scribejava.core.oauth2.bearersignature.{BearerSignature,
  BearerSignatureURIQueryParameter}
import com.github.scribejava.core.oauth2.clientauthentication.{ClientAuthentication,
  RequestBodyAuthenticationScheme}
import com.github.scribejava.httpclient.ning.NingHttpClient
import com.ning.http.client.{AsyncHttpClient, AsyncHttpClientConfig}
import com.typesafe.config.Config
import io.gearpump.security.Authenticator
import io.gearpump.services.SecurityService.UserSession
import io.gearpump.services.security.oauth2.OAuth2Authenticator
import io.gearpump.services.security.oauth2.impl.BaseOAuth2Authenticator.BaseApi20
import io.gearpump.util.Constants._
import io.gearpump.util.Util
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
 * Uses Ning AsyncClient to connect to OAuth2 service.
 *
 * See [[io.gearpump.services.security.oauth2.OAuth2Authenticator]]
 * for more API information.
 */
abstract class BaseOAuth2Authenticator extends OAuth2Authenticator {

  // Authorize Url for end user to authorize
  protected def authorizeUrl: String

  // Used to fetch the Access Token.
  protected def accessTokenEndpoint: String

  // Protected resource Url to get the user profile
  protected def protectedResourceUrl: String

  // Extracts the username information from response of protectedResourceUrl
  protected def extractUserName(body: String): String

  // Scope required to access protectedResourceUrl
  protected def scope: String

  // OAuth2 endpoint definition for ScribeJava.
  protected def oauth2Api(): DefaultApi20 = {
    new BaseApi20(authorizeUrl, accessTokenEndpoint)
  }

  protected var oauthService: OAuth20Service = null

  protected var asyncHttpClient: AsyncHttpClient = null

  protected var executionContext: ExecutionContext = null

  private var oauthState: String = null

  private var defaultPermissionLevel = Authenticator.Guest.permissionLevel

  // Synchronization ensured by the caller
  override def init(config: Config, executionContext: ExecutionContext): Unit = {
    if (this.oauthService == null) {
      val callback = config.getString(GEARPUMP_UI_OAUTH2_AUTHENTICATOR_CALLBACK)
      val clientId = config.getString(GEARPUMP_UI_OAUTH2_AUTHENTICATOR_CLIENT_ID)
      val clientSecret = config.getString(GEARPUMP_UI_OAUTH2_AUTHENTICATOR_CLIENT_SECRET)
      defaultPermissionLevel = {
        val role = config.getString(GEARPUMP_UI_OAUTH2_AUTHENTICATOR_DEFAULT_USER_ROLE)
        role match {
          case "guest" => Authenticator.Guest.permissionLevel
          case "user" => Authenticator.User.permissionLevel
          case "admin" => Authenticator.Admin.permissionLevel
          case _ => Authenticator.UnAuthenticated.permissionLevel
        }
      }
      this.oauthService = buildOAuth2Service(clientId, clientSecret, callback)
      this.executionContext = executionContext
    }
  }

  private val isClosed: AtomicBoolean = new AtomicBoolean(false)

  override def close(): Unit = {
    if (isClosed.compareAndSet(false, true)) {
      if (null != oauthService) {
        oauthService.close()
      }
    }
  }

  override def getAuthorizationUrl: String = {
    oauthService.getAuthorizationUrl(oauthState)
  }

  protected def authenticateWithAccessToken(accessToken: OAuth2AccessToken): Future[UserSession] = {
    val promise = Promise[UserSession]()
    val request = new OAuthRequest(Verb.GET, protectedResourceUrl)
    oauthService.signRequest(accessToken, request)
    oauthService.execute(request,
      new OAuthAsyncRequestCallback[Response] {
        override def onCompleted(response: Response): Unit = {
          try {
            val user = extractUserName(response.getBody)
            promise.success(new UserSession(user, defaultPermissionLevel))
          } catch {
            case ex: Throwable =>
              promise.failure(ex)
          }
        }

        override def onThrowable(throwable: Throwable): Unit = {
          promise.failure(throwable)
        }
      })
    promise.future
  }

  protected def accessTokenRequestParams(code: String): AccessTokenRequestParams = {
    AccessTokenRequestParams.create(code)
  }

  protected def authenticateWithAuthorizationCode(code: String): Future[UserSession] = {

    implicit val ec: ExecutionContext = executionContext

    val promise = Promise[UserSession]()
    oauthService.getAccessToken(accessTokenRequestParams(code),
      new OAuthAsyncRequestCallback[OAuth2AccessToken] {
        override def onCompleted(accessToken: OAuth2AccessToken): Unit = {
          authenticateWithAccessToken(accessToken).onComplete {
            case Success(user) => promise.success(user)
            case Failure(ex) => promise.failure(ex)
          }
        }

        override def onThrowable(throwable: Throwable): Unit = {
          promise.failure(throwable)
        }
      })
    promise.future
  }

  override def authenticate(parameters: Map[String, String]): Future[UserSession] = {

    val code = parameters.get(GEARPUMP_UI_OAUTH2_AUTHENTICATOR_AUTHORIZATION_CODE)
    val accessToken = parameters.get(GEARPUMP_UI_OAUTH2_AUTHENTICATOR_ACCESS_TOKEN)

    if (accessToken.isDefined) {
      authenticateWithAccessToken(new OAuth2AccessToken(accessToken.get))
    } else if (code.isDefined) {
      authenticateWithAuthorizationCode(code.get)
    } else {
      // Fails authentication if code not exist
      Future.failed(new Exception("Fail to authenticate user as there is no code parameter in URL"))
    }
  }

  private def buildOAuth2Service(clientId: String, clientSecret: String, callback: String)
    : OAuth20Service = {
    oauthState = "state" + Util.randInt()
    val clientConfig: AsyncHttpClientConfig = new AsyncHttpClientConfig.Builder()
      .setMaxConnections(5)
      .setUseProxyProperties(true)
      .setRequestTimeout(60000)
      .setAllowPoolingConnections(false)
      .setPooledConnectionIdleTimeout(60000)
      .setReadTimeout(60000).build

    asyncHttpClient = new AsyncHttpClient(clientConfig)

    val service: OAuth20Service = new ServiceBuilder(clientId)
      .apiSecret(clientSecret)
      .defaultScope(scope)
      .callback(callback)
      .httpClient(new NingHttpClient(asyncHttpClient))
      .build(oauth2Api())

    service
  }
}

object BaseOAuth2Authenticator {

  class BaseApi20(authorizeUrl: String, accessTokenEndpoint: String,
      clientAuthentication: ClientAuthentication = RequestBodyAuthenticationScheme.instance())
    extends DefaultApi20 {

    override def getAccessTokenEndpoint: String = {
      accessTokenEndpoint
    }

    protected override def getAuthorizationBaseUrl: String = {
      authorizeUrl
    }

    override def getClientAuthentication: ClientAuthentication = {
      clientAuthentication
    }

    override def getBearerSignature: BearerSignature = {
      BearerSignatureURIQueryParameter.instance()
    }
  }
}
