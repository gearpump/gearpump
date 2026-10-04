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

  private val passwordVerifier = new PasswordVerificationExecutor(2, 16)
  private val dummyPasswordHash = PasswordUtil.hash("gearpump-unknown-account-placeholder")

  private val ROOT = "gearpump.ui-security.config-file-based-authenticator"
  private val ADMINS = ROOT + "." + "admins"
  private val USERS = ROOT + "." + "users"
  private val GUESTS = ROOT + "." + "guests"

  private[security] case class Credentials(
      admins: Map[String, String], users: Map[String, String], guests: Map[String, String]) {

    def verify(user: String, password: String,
        verifyPassword: (String, String) => Boolean = PasswordUtil.verify _)
        : AuthenticationResult = {
      val account: Option[(String, AuthenticationResult)] =
        admins.get(user).map(_ -> Authenticator.Admin)
          .orElse(users.get(user).map(_ -> Authenticator.User))
          .orElse(guests.get(user).map(_ -> Authenticator.Guest))
      // Unknown users perform the same password check, but can never become authenticated.
      val stored = account.map(_._1).getOrElse(dummyPasswordHash)
      val valid = verifyPassword(password, stored)
      account.filter(_ => valid).map(_._2).getOrElse(Authenticator.UnAuthenticated)
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
 * Passwords are stored as versioned PBKDF2-HMAC-SHA256 hashes with cryptographic salts.
 * Legacy SHA-1 hashes are rejected; generate replacements with PasswordUtil before upgrading.
 */
class ConfigFileBasedAuthenticator(config: Config) extends Authenticator {

  private val credentials = loadCredentials(config)

  override def authenticate(user: String, password: String, ec: ExecutionContext)
    : Future[AuthenticationResult] = {
    passwordVerifier.verify(credentials.verify(user, password))
  }

  private def loadCredentials(config: Config): Credentials = {
    val admins = configToMap(config, ADMINS)
    val users = configToMap(config, USERS)
    val guests = configToMap(config, GUESTS)
    Seq(admins, users, guests).foreach { role =>
      role.foreach { case (user, digest) =>
        require(PasswordUtil.isSupportedHash(digest),
          s"Unsupported password hash for $user; regenerate it with PasswordUtil")
      }
    }
    new Credentials(admins, users, guests)
  }

  private def configToMap(config: Config, path: String) = {
    import scala.jdk.CollectionConverters._
    config.getConfig(path).root.unwrapped.asScala.toMap map { case (k, v) => k -> v.toString }
  }
}
