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

import com.typesafe.config.Config
import io.gearpump.services.SecurityService.UserSession
import io.gearpump.util.Constants
import io.gearpump.util.Constants._
import scala.concurrent.{ExecutionContext, Future}

/**
 *
 * Uses OAuth2 social-login as the mechanism for authentication.
 * @see [[https://tools.ietf.org/html/rfc6749]] to find what is OAuth2, and how it works.
 *
 * Basically flow for OAuth2 Authentication:
 *  1. User accesses Gearpump UI website, and choose to login with OAuth2 server.
 *  2. Gearpump UI website redirects user to OAuth2 server domain authorization endpoint.
 *  3. End user complete the authorization in the domain of OAuth2 server.
 *  4. OAuth2 server redirects user back to Gearpump UI server.
 *  5. Gearpump UI server verify the tokens and extract credentials from query
 *    parameters and form fields.
 *
 * NOTE: '''Thread-safety''' is a MUST requirement. Developer need to ensure the sub-class is
 *      thread-safe. Sub-class should have a parameterless constructor.
 *
 * NOTE:  OAuth2 Authenticator requires access of Internet. Please make sure HTTP proxy are
 * set properly if applied.
 *
 * Example: Config proxy when UI server is started on Windows:
 * {{{
 *   > set JAVA_OPTS=-Dhttp.proxyHost=xx.com -Dhttp.proxyPort=8088 -Dhttps.proxyHost=xx.com
 *      -Dhttps.proxyPort=8088
 *   > bin\services
 * }}}
 *
 * Example: Config proxy when UI server is started on Linux:
 * {{{
 *   $ export JAVA_OPTS="-Dhttp.proxyHost=xx.com -Dhttp.proxyPort=8088 -Dhttps.proxyHost=xx.com
 *      -Dhttps.proxyPort=8088"
 *   $ bin/services
 * }}}
 */
trait OAuth2Authenticator {

  /**
   * Inits authenticator with config which contains client ID, client secret, and etc..
   *
   * Typically, the client key and client secret is provided by OAuth2 Authorization server
   * when user register an application there.
   * See [[https://tools.ietf.org/html/rfc6749]] for definition of client, client Id,
   * and client secret.
   *
   * See [[https://developer.github.com/v3/oauth/]] for an actual example of how Github
   * use client key, and client secret.
   *
   * NOTE:  '''Thread-Safety''': Framework ensures this call is synchronized.
   *
   * @param config Client Id, client secret, callback URL and etc..
   * @param executionContext ExecutionContext from hosting environment.
   */
  def init(config: Config, executionContext: ExecutionContext): Unit

  /**
   * Returns the OAuth Authorization URL so for redirection to that address to do OAuth2
   * authorization.
   *
   * NOTE:  '''Thread-Safety''': This can be called in a multi-thread environment. Developer
   *      need to ensure thread safety.
   */
  def getAuthorizationUrl: String

  /**
   * After authorization, OAuth2 server redirects user back with tokens. This verify the
   * tokens, retrieve the profiles, and return [[UserSession]] information.
   *
   * NOTE:  This is an Async call.
   *
   * NOTE:  This call requires external internet access.
   *
   * NOTE:  '''Thread-Safety''': This can be called in a multi-thread environment. Developer
   *      need to ensure thread safety.
   *
   * @param parameters HTTP Query and Post parameters, which typically contains Authorization code.
   * @return UserSession if authentication pass.
   */
  def authenticate(parameters: Map[String, String]): Future[UserSession]

  /**
   * Clean up resource
   */
  def close(): Unit
}

object OAuth2Authenticator {

  // Serves as a quick immutable lookup cache
  private var providers = Map.empty[String, OAuth2Authenticator]

  /**
   * Load Authenticator from [[Constants.GEARPUMP_UI_OAUTH2_AUTHENTICATORS]]
   *
   * @param provider, Name for the OAuth2 Authentication Service.
   * @return Returns null if the OAuth2 Authentication is disabled.
   */
  def get(config: Config, provider: String, executionContext: ExecutionContext)
    : OAuth2Authenticator = {

    if (providers.contains(provider)) {
      providers(provider)
    } else {
      val path = s"${Constants.GEARPUMP_UI_OAUTH2_AUTHENTICATORS}.$provider"
      val enabled = config.getBoolean(Constants.GEARPUMP_UI_OAUTH2_AUTHENTICATOR_ENABLED)
      if (enabled && config.hasPath(path)) {
        this.synchronized {
          if (providers.contains(provider)) {
            providers(provider)
          } else {
            val authenticatorConfig = config.getConfig(path)
            val authenticatorClass = authenticatorConfig.getString(
              GEARPUMP_UI_OAUTH2_AUTHENTICATOR_CLASS)
            val clazz = Thread.currentThread().getContextClassLoader.loadClass(authenticatorClass)
            val authenticator = clazz.newInstance().asInstanceOf[OAuth2Authenticator]
            authenticator.init(authenticatorConfig, executionContext)
            providers += provider -> authenticator
            authenticator
          }
        }
      } else {
        null
      }
    }
  }
}