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

package io.gearpump.security

import io.gearpump.security.Authenticator.AuthenticationResult
import scala.concurrent.{ExecutionContext, Future}

/**
 * Authenticator for UI dashboard.
 *
 * Sub Class must implement a constructor with signature like this:
 * this(config: Config)
 */
trait Authenticator {

  // TODO: Change the signature to return more attributes of user credentials...
  def authenticate(
      user: String, password: String, ec: ExecutionContext): Future[AuthenticationResult]
}

object Authenticator {

  trait AuthenticationResult {

    def authenticated: Boolean

    def permissionLevel: Int
  }

  val UnAuthenticated = new AuthenticationResult {
    override val authenticated = false
    override val permissionLevel = -1
  }

  /** Guest can view but have no permission to submit app or write */
  val Guest = new AuthenticationResult {
    override val authenticated = true
    override val permissionLevel = 1000
  }

  /** User can submit app, kill app, but have no permission to add or remote machines */
  val User = new AuthenticationResult {
    override val authenticated = true
    override val permissionLevel = 1000 + Guest.permissionLevel
  }

  /** Super user */
  val Admin = new AuthenticationResult {
    override val authenticated = true
    override val permissionLevel = 1000 + User.permissionLevel
  }
}