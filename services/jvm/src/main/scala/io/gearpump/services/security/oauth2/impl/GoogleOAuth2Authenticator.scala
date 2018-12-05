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

import com.github.scribejava.apis.google.GoogleJsonTokenExtractor
import com.github.scribejava.core.builder.api.DefaultApi20
import com.github.scribejava.core.extractors.TokenExtractor
import com.github.scribejava.core.model._
import spray.json._

/**
 *
 * Does authentication with Google OAuth2 service. It only extract the email address
 * from user profile of Google.
 *
 * Pre-requisite steps to use this Authenticator:
 *
 * Step1: Register your website as an OAuth2 Application on Google
 *  1) Create an application representing your website at [[https://console.developers.google.com]]
 *
 *  2) In "API Manager" of your created application, enable API "Google+ API"
 *
 *  3) Create OAuth client ID for this application. In "Credentials" tab of "API Manager",
 * choose "Create credentials", and then select OAuth client ID. Follow the wizard
 * to set callback URL, and generate client ID, and client Secret. Callback URL is NOT optional.
 *
 * Step2: Configure the OAuth2 information in gear.conf
 *
 *  1) Enable OAuth2 authentication by setting "gearpump.ui-security.oauth2-authenticator-enabled"
 * as true.
 *
 *  2) Configure section "gearpump.ui-security.oauth2-authenticators.google". Please make sure
 * class name, client ID, client Secret, and callback URL are set properly.
 *
 * NOTE:  callback URL set here should match what is configured on Google in step1.
 *
 * Step3: Restart the UI service and try out the Google social login button in UI.
 *
 * NOTE:  OAuth requires Internet access, @see
 *       [[io.gearpump.services.security.oauth2.OAuth2Authenticator]] to find
 *       some helpful tutorials
 *
 * NOTE:  Google use scope to define what data can be fetched by OAuth2. Currently we use profile
 *       [[https://www.googleapis.com/auth/userinfo.email]]. However, Google may change the profile
 *       in future.
 *
 * TODO Currently, this doesn't verify the state from Google OAuth2 response.
 *
 * See [[io.gearpump.services.security.oauth2.OAuth2Authenticator]] for more
 * API information.
 */
class GoogleOAuth2Authenticator extends BaseOAuth2Authenticator {

  import io.gearpump.services.security.oauth2.impl.GoogleOAuth2Authenticator._

  protected override def authorizeUrl: String = AuthorizeUrl

  protected override def accessTokenEndpoint: String = AccessEndpoint

  protected override def protectedResourceUrl: String = ResourceUrl

  protected override def scope: String = GoogleOAuth2Authenticator.Scope

  protected override def extractUserName(body: String): String = {
    val emails = body.parseJson.asJsObject.fields("emails").asInstanceOf[JsArray]
    val email = emails.elements(0).asJsObject("Cannot find email account")
      .fields("value").asInstanceOf[JsString].value
    email
  }

  override def oauth2Api(): DefaultApi20 = new AsyncGoogleApi20(authorizeUrl, accessTokenEndpoint)
}

object GoogleOAuth2Authenticator {

  import io.gearpump.services.security.oauth2.impl.BaseOAuth2Authenticator._

  // scalastyle:off line.size.limit
  val AuthorizeUrl = "https://accounts.google.com/o/oauth2/auth?response_type=%s&client_id=%s&redirect_uri=%s&scope=%s"
  // scalastyle:on line.size.limit
  val AccessEndpoint = "https://www.googleapis.com/oauth2/v4/token"
  val ResourceUrl = "https://www.googleapis.com/plus/v1/people/me"
  val Scope = "https://www.googleapis.com/auth/userinfo.email"

  private class AsyncGoogleApi20(authorizeUrl: String, accessEndpoint: String)
    extends BaseApi20(authorizeUrl, accessEndpoint) {

    override def getAccessTokenExtractor: TokenExtractor[OAuth2AccessToken] = {
      GoogleJsonTokenExtractor.instance
    }
  }
}