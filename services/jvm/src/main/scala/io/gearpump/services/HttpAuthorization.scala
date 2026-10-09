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

import io.gearpump.security.Authenticator
import java.util.regex.Pattern
import org.apache.pekko.http.scaladsl.model.Uri

private[services] object HttpAuthorization {
  private val apiPrefix = s"/*api/+${Pattern.quote(REST_VERSION)}"
  private val terminationRoute = Pattern.compile("/*terminate/*")
  private val supervisorMutationRoute =
    Pattern.compile(s"$apiPrefix/+supervisor/+(?:addworker|removeworker)(?:/.*)?")
  private val configRoute = Pattern.compile(s"$apiPrefix(?:/+[^/]+)*/+config/*")
  private val adminRoutes = Seq(terminationRoute, supervisorMutationRoute, configRoute)

  def requiredPermission(path: Uri.Path): Int = {
    val renderedPath = path.toString
    if (adminRoutes.exists(_.matcher(renderedPath).matches())) {
      Authenticator.Admin.permissionLevel
    } else {
      Authenticator.Guest.permissionLevel
    }
  }
}
