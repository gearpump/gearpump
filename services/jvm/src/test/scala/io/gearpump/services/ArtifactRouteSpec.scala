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

import com.typesafe.config.Config
import io.gearpump.cluster.TestUtil
import io.gearpump.jarstore.{FileServer, JarStore}
import io.gearpump.jarstore.local.LocalJarStore
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ArtifactRouteSpec extends AnyFlatSpec with Matchers with ScalatestRouteTest {
  override def testConfig: Config = TestUtil.DEFAULT_CONFIG
  private def store(): LocalJarStore = {
    val store = new LocalJarStore
    val root = java.nio.file.Files.createTempDirectory("artifact-auth-")
    store.init(TestUtil.DEFAULT_CONFIG.withValue("gearpump.jarstore.rootpath",
      com.typesafe.config.ConfigValueFactory.fromAnyRef(root.toString)))
    store
  }
  it should "deny missing and incorrect artifact credentials before processing uploads" in {
    val underlying = store()
    val server = new FileServer(system, "localhost", 0, underlying)
    Post("/upload") ~> server.route ~> check { assert(!handled) }
    Post("/upload").addHeader(RawHeader("Authorization", "Bearer wrong")) ~>
      server.route ~> check { assert(!handled) }
    assert(underlying.listFiles().isEmpty)
  }
}
