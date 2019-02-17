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

import com.typesafe.config.Config
import io.gearpump.security.Authenticator.AuthenticationResult
import io.gearpump.security.ConfigFileBasedAuthenticator._
import scala.concurrent.{ExecutionContext, Future}

object ConfigFileBasedAuthenticator {

  private val ROOT = "gearpump.ui-security.config-file-based-authenticator"
  private val ADMINS = ROOT + "." + "admins"
  private val USERS = ROOT + "." + "users"
  private val GUESTS = ROOT + "." + "guests"

  private case class Credentials(
      admins: Map[String, String], users: Map[String, String], guests: Map[String, String]) {

    def verify(user: String, password: String): AuthenticationResult = {
      if (admins.contains(user)) {
        if (verify(user, password, admins)) {
          Authenticator.Admin
        } else {
          Authenticator.UnAuthenticated
        }
      } else if (users.contains(user)) {
        if (verify(user, password, users)) {
          Authenticator.User
        } else {
          Authenticator.UnAuthenticated
        }
      } else if (guests.contains(user)) {
        if (verify(user, password, guests)) {
          Authenticator.Guest
        } else {
          Authenticator.UnAuthenticated
        }
      } else {
        Authenticator.UnAuthenticated
      }
    }

    private def verify(user: String, password: String, map: Map[String, String]): Boolean = {
      val storedPass = map(user)
      PasswordUtil.verify(password, storedPass)
    }
  }
}

/**
 * UI dashboard authenticator based on configuration file.
 *
 * It has three categories of users: admins, users, and guests.
 * admins have unlimited permission, like shutdown a cluster, add/remove machines.
 * users have limited permission to submit an application and etc..
 * guests can not submit/kill applications, but can view the application status.
 *
 * see conf/gear.conf section gearpump.ui-security.config-file-based-authenticator to find
 * information about how to configure this authenticator.
 *
 * [Security consideration]
 * It will keep one-way sha1 digest of password instead of password itself. The original password is
 * NOT kept in any way, so generally it is safe.
 *
 *
 * digesting flow (from original password to digest):
 * {{{
 * random salt byte array of length 8 -> byte array of (salt + sha1(salt, password)) ->
 * base64Encode.
 * }}}
 *
 * Verification user input password with stored digest:
 * {{{
 * base64Decode -> extract salt -> do sha1(salt, password) -> generate digest:
 * salt + sha1 -> compare the generated digest with the stored digest.
 * }}}
 */
class ConfigFileBasedAuthenticator(config: Config) extends Authenticator {

  private val credentials = loadCredentials(config)

  override def authenticate(user: String, password: String, ec: ExecutionContext)
    : Future[AuthenticationResult] = {
    implicit val ctx = ec
    Future {
      credentials.verify(user, password)
    }
  }

  private def loadCredentials(config: Config): Credentials = {
    val admins = configToMap(config, ADMINS)
    val users = configToMap(config, USERS)
    val guests = configToMap(config, GUESTS)
    new Credentials(admins, users, guests)
  }

  private def configToMap(config: Config, path: String) = {
    import scala.collection.JavaConverters._
    config.getConfig(path).root.unwrapped.asScala.toMap map { case (k, v) => k -> v.toString }
  }
}