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

import com.typesafe.config.ConfigFactory
import io.gearpump.jarstore.local.LocalJarStore
import java.nio.file.Files
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LocalJarStoreSpec extends AnyFlatSpec with Matchers {
  it should "reject absolute, URI-qualified, and traversal artifact names" in {
    val root = Files.createTempDirectory("jarstore-test-")
    val store = new LocalJarStore
    store.init(ConfigFactory.empty().withValue("gearpump.jarstore.rootpath",
      com.typesafe.config.ConfigValueFactory.fromAnyRef(root.toString)))
    Seq("../secret", "/tmp/secret", "hdfs://host/file", ".", "..", "a/b", "a\\b")
      .foreach { name =>
        intercept[IllegalArgumentException] { store.getFile(name) }
        intercept[IllegalArgumentException] { store.createFile(name) }
      }
    Files.delete(root)
  }

  it should "read ordinary artifacts and refuse symlink reads or overwrites" in {
    val root = Files.createTempDirectory("jarstore-test-")
    val external = Files.createTempFile("outside-artifact-", ".txt")
    val store = new LocalJarStore
    store.init(ConfigFactory.empty().withValue("gearpump.jarstore.rootpath",
      com.typesafe.config.ConfigValueFactory.fromAnyRef(root.toString)))
    val output = store.createFile("artifact.jar")
    output.write(Array[Byte](1, 2, 3))
    output.close()
    val input = store.getFile("artifact.jar")
    try assert(input.read() == 1) finally input.close()
    val link = root.resolve("link.jar")
    Files.createSymbolicLink(link, external)
    intercept[java.io.IOException] { store.getFile("link.jar") }
    intercept[java.io.IOException] { store.createFile("link.jar") }
    intercept[java.io.IOException] { store.createFile("artifact.jar") }
    Files.delete(link)
    Files.delete(root.resolve("artifact.jar"))
    Files.delete(root)
    Files.delete(external)
  }
}
