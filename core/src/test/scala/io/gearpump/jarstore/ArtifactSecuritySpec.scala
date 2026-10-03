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

package io.gearpump.jarstore

import com.typesafe.config.Config
import io.gearpump.cluster.TestUtil
import io.gearpump.jarstore.local.LocalJarStore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ArtifactSecuritySpec extends AnyFlatSpec with Matchers {
  private def store(): LocalJarStore = {
    val store = new LocalJarStore
    val root = java.nio.file.Files.createTempDirectory("quota-regression-")
    store.init(TestUtil.DEFAULT_CONFIG.withValue("gearpump.jarstore.rootpath",
      com.typesafe.config.ConfigValueFactory.fromAnyRef(root.toString)))
    store
  }
  it should "count existing artifacts, reserve concurrent capacity and recover on deletion" in {
    val underlying = store()
    val first = underlying.createFile("existing.jar")
    first.write(Array[Byte](1, 2, 3)); first.close()
    val quota = new QuotaJarStore(underlying, 10, 2, 6)
    val active = quota.createFile("active.jar")
    intercept[java.io.IOException] { quota.createFile("parallel.jar") }
    active.write(Array[Byte](1, 2, 3, 4)); active.close()
    intercept[java.io.IOException] { quota.createFile("excess.jar") }
    val restarted = new QuotaJarStore(underlying, 10, 2, 6)
    intercept[java.io.IOException] { restarted.createFile("excess.jar") }
    restarted.deleteFile("existing.jar")
    val next = restarted.createFile("next.jar")
    next.write(1); next.close()
    assert(restarted.listFiles().values.sum == 5)
  }
  it should "delete empty and oversized artifacts and release their reservations" in {
    val underlying = store()
    val quota = new QuotaJarStore(underlying, 6, 1, 6)
    quota.createFile("empty.jar").close()
    assert(underlying.listFiles().isEmpty)
    val oversized = quota.createFile("oversized.jar")
    intercept[java.io.IOException] { oversized.write(new Array[Byte](7)) }
    oversized.close()
    assert(underlying.listFiles().isEmpty)
    val accepted = quota.createFile("accepted.jar")
    accepted.write(1); accepted.close()
    assert(underlying.listFiles() == Map("accepted.jar" -> 1L))
  }
}
