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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PasswordUtilSpec extends AnyFlatSpec with Matchers {

  it should "verify the credential correctly" in {
    val password = "password"

    val digest1 = PasswordUtil.hash(password)
    val digest2 = PasswordUtil.hash(password)

    // Uses different salt each time, thus creating different hash.
    assert(digest1 != digest2)

    // Both are valid hash.
    assert(PasswordUtil.verify(password, digest1))
    assert(PasswordUtil.verify(password, digest2))
  }

  it should "reject wrong passwords, malformed hashes, and legacy digests" in {
    val digest = PasswordUtil.hash("correct password")
    assert(digest.startsWith("pbkdf2-sha256:600000:"))
    assert(!PasswordUtil.verify("wrong password", digest))
    Seq(null, "", "not-base64", "AeGxGOxlU8QENdOXejCeLxy+isrCv0TrS37HwA==",
      digest.replace(":600000:", ":1:"), digest.replace(":600000:", ":2147483647:"),
      digest.replace("pbkdf2-sha256", "unknown"), digest + ":extra").foreach { stored =>
      assert(!PasswordUtil.verify("correct password", stored))
    }
  }

  it should "support unicode passwords" in {
    val password = "\u5bc6\u7801-\ud83d\udd10"
    assert(PasswordUtil.verify(password, PasswordUtil.hash(password)))
  }
}
