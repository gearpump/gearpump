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

package io.gearpump.services.security

import com.typesafe.config.Config
import org.apache.pekko.http.scaladsl.model.Uri
import io.gearpump.util.Constants

private[services] object DashboardDeployment {
  def validate(config: Config): Unit = {
    require(config.getBoolean(Constants.GEARPUMP_UI_SECURITY_AUTHENTICATION_ENABLED),
      "Dashboard authentication is required")
    val nativeTls = config.getBoolean("gearpump.services.https-enabled")
    val proxy = config.getBoolean("gearpump.services.tls-termination-proxy")
    val host = config.getString(Constants.GEARPUMP_SERVICE_HOST)
    require(nativeTls != proxy, "Choose native HTTPS or explicit loopback TLS proxy mode")
    require(!proxy || Set("127.0.0.1", "::1").contains(host),
      "TLS proxy mode must bind to a numeric loopback address")
    val origin = Uri(config.getString("gearpump.services.public-origin"))
    require(origin.scheme == "https" && origin.authority.host.address().nonEmpty &&
      origin.authority.userinfo.isEmpty && origin.path.isEmpty &&
      origin.rawQueryString.isEmpty && origin.fragment.isEmpty,
      "public-origin must be an HTTPS origin without userinfo, path, query or fragment")
    require(config.getBoolean("pekko.http.session.cookie.secure") &&
      config.getBoolean("pekko.http.session.cookie.http-only") &&
      config.getBoolean("pekko.http.session.csrf.cookie.secure") &&
      config.getString("pekko.http.session.csrf.cookie.name") == "__Host-XSRF-TOKEN" &&
      config.getString("pekko.http.session.csrf.cookie.path") == "/" &&
      config.getString("pekko.http.session.csrf.cookie.domain") == "none",
      "Session and CSRF cookies must retain their secure deployment settings")
  }
}
