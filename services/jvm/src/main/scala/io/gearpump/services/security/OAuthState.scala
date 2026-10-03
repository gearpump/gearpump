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

import java.security.{MessageDigest, SecureRandom}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64
import scala.collection.mutable

/** Bounded, expiring, single-use login attempts bound to a browser and provider. */
private[services] class OAuthState(now: () => Long = () => System.nanoTime(),
    ttlNanos: Long = 300000000000L, capacity: Int = 1024) {
  case class Attempt(state: String, browser: String)
  private case class Pending(browser: String, provider: String, created: Long)
  private val random = new SecureRandom()
  private val pending = mutable.Map.empty[String, Pending]
  private def token(): String = {
    val bytes = new Array[Byte](32)
    random.nextBytes(bytes)
    Base64.getUrlEncoder.withoutPadding().encodeToString(bytes)
  }
  private def expire(): Unit = {
    val current = now()
    pending.filterInPlace { case (_, value) => current - value.created < ttlNanos }
  }
  def begin(provider: String): Option[Attempt] = synchronized {
    expire()
    if (pending.size >= capacity) None
    else {
      val attempt = Attempt(token(), token())
      pending(attempt.state) = Pending(attempt.browser, provider, now())
      Some(attempt)
    }
  }
  def consume(state: String, browser: String, provider: String): Boolean = synchronized {
    expire()
    if (state.length != 43 || browser.length != 43) false
    else pending.get(state) match {
      case Some(value) if value.provider == provider && MessageDigest.isEqual(
          value.browser.getBytes(UTF_8), browser.getBytes(UTF_8)) =>
        pending.remove(state)
        true
      case _ => false
    }
  }
}
