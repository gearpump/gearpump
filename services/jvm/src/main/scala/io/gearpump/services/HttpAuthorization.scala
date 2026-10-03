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
import org.apache.pekko.http.scaladsl.model.Uri

private[services] object HttpAuthorization {
  def requiredPermission(path: Uri.Path): Int = {
    def segments(rest: Uri.Path): List[String] = rest match {
      case Uri.Path.Empty => Nil
      case Uri.Path.Slash(tail) => segments(tail)
      case Uri.Path.Segment(head, tail) => head :: segments(tail)
    }
    val parts = segments(path)
    val admin = parts == List("terminate") ||
      parts.take(3) == List("api", REST_VERSION, "supervisor") ||
      (parts.take(2) == List("api", REST_VERSION) && parts.lastOption.contains("config"))
    if (admin) Authenticator.Admin.permissionLevel else Authenticator.Guest.permissionLevel
  }
}
