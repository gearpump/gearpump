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

import org.scalatest.{FlatSpec, Matchers}

class PasswordUtilSpec extends FlatSpec with Matchers {

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
}
