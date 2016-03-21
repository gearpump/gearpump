/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.services.security.oauth2.impl

import com.github.scribejava.core.builder.api.DefaultApi20
import com.github.scribejava.core.model.{AbstractRequest, OAuthConfig, OAuthConstants}
import com.github.scribejava.core.oauth.OAuth20Service
import com.typesafe.config.Config
import io.gearpump.services.security.oauth2.OAuth2Authenticator
import io.gearpump.services.security.oauth2.impl.BaseOAuth2Authenticator.BaseApi20
import io.gearpump.services.security.oauth2.impl.CloudFoundryUAAOAuth2Authenticator.CloudFoundryUAAService
import spray.json.{JsString, _}
import sun.misc.BASE64Encoder

/**
 *
 * Does authentication with CloudFoundry UAA service. Currently it only
 * extract the email address of end user.
 *
 * For what is UAA, please see:
 * @see [[https://github.com/cloudfoundry/uaa for information about CloudFoundry UAA]]
 *      (User Account and Authentication Service)
 *
 * Pre-requisite steps to use this Authenticator:
 *
 * Step1: Register your website to UAA with tool uaac.
 * 1) Check tutorial on uaac at [[https://docs.cloudfoundry.org/adminguide/uaa-user-management.html]]
 * 2) Open a bash shell, and login in as user admin by
 * {{{
 *    uaac token client get admin -s MyAdminPassword
 * }}}
 * 3) Create a new Application (Client) in UAA,
 * {{{
 *   uaac client add [your_client_id]
 *     --scope openid
 *     --authorized_grant_types "authorization_code client_credentials refresh_token"
 *     --authorities openid
 *     --redirect_uri [your_redirect_url]
 *     --autoapprove true
 *     --secret [your_client_secret]
 * }}}
 *
 * Step2: Configure the OAuth2 information in gear.conf
 * 1) Enable OAuth2 authentication by setting "gearpump.ui-security.oauth2-authenticator-enabled"
 * as true.
 * 2) Navigate to section "gearpump.ui-security.oauth2-authenticators.cloudfoundryuaa"
 * 3) Config gear.conf "gearpump.ui-security.oauth2-authenticators.cloudfoundryuaa" section.
 * Please make sure class name, client ID, client Secret, and callback URL are set properly.
 *
 * @note The callback URL here should matche what you set on CloudFoundry UAA in step1.
 *
 * Step3: Restart the UI service and try the "social login" button for UAA.
 *
 * @note OAuth requires Internet access, @see [[OAuth2Authenticator]] to find tutorials to configure
 *       Internet proxy.
 *
 * @see [[OAuth2Authenticator]] for more background information of OAuth2.
 */
class CloudFoundryUAAOAuth2Authenticator extends BaseOAuth2Authenticator {

  private var host: String = null

  protected override def authorizeUrl: String = s"$host/oauth/authorize?response_type=%s&client_id=%s&redirect_uri=%s&scope=%s"

  protected override def accessTokenEndpoint: String = s"$host/oauth/token"

  protected override def protectedResourceUrl: String = s"$host/userinfo"

  protected override def scope: String = "openid"

  override def init(config: Config): Unit = {
    host = config.getString("uaahost")
    super.init(config)
  }

  protected override def extractUserName(body: String): String = {
    val email = body.parseJson.asJsObject.fields("email").asInstanceOf[JsString]
    email.value
  }


  protected override def oauth2Api(): DefaultApi20 = {
    new CloudFoundryUAAService(authorizeUrl, accessTokenEndpoint)
  }
}

object CloudFoundryUAAOAuth2Authenticator {
  private val RESPONSE_TYPE = "response_type"

  private class CloudFoundryUAAService(authorizeUrl: String, accessTokenEndpoint: String)
    extends BaseApi20(authorizeUrl, accessTokenEndpoint) {

    private def base64(in: String): String = {
      val encoder = new BASE64Encoder()
      val utf8 = "UTF-8"
      encoder.encode(in.getBytes(utf8))
    }

    override def createService(config: OAuthConfig): OAuth20Service = {
      return new OAuth20Service(this, config) {

        protected override def createAccessTokenRequest[T <: AbstractRequest](code: String, request: T): T = {
          val config: OAuthConfig = getConfig()

          request.addParameter(OAuthConstants.GRANT_TYPE, OAuthConstants.AUTHORIZATION_CODE)
          request.addParameter(OAuthConstants.CODE, code)
          request.addParameter(RESPONSE_TYPE, "token")
          request.addParameter(OAuthConstants.REDIRECT_URI, config.getCallback)

          // Work around issue https://github.com/scribejava/scribejava/issues/641
          request.addHeader("Content-Type", "application/x-www-form-urlencoded")

          // CloudFoundry requires a Authorization header encoded with client Id and secret.
          val authorizationHeader = "Basic " + base64(config.getApiKey + ":" + config.getApiSecret)
          request.addHeader("Authorization", authorizationHeader)
          request
        }
      }
    }
  }
}