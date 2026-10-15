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

package io.gearpump.jarstore.dfs

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import java.nio.file.Files
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DFSJarStoreSpec extends AnyFlatSpec with Matchers {
  it should "contain artifact names when using a Hadoop filesystem backend" in {
    val root = Files.createTempDirectory("dfs-artifact-test-")
    val store = new DFSJarStore
    store.init(ConfigFactory.empty().withValue("gearpump.jarstore.rootpath",
      ConfigValueFactory.fromAnyRef(root.toUri.toString)))
    Seq("../outside", "/etc/passwd", "file:///etc/passwd", "hdfs://other/root", "a/b", "..")
      .foreach { name =>
        intercept[IllegalArgumentException] { store.getFile(name) }
        intercept[IllegalArgumentException] { store.createFile(name) }
      }
    val output = store.createFile("artifact.jar")
    output.write(Array[Byte](1, 2, 3))
    output.close()
    val input = store.getFile("artifact.jar")
    try assert(input.read() == 1) finally input.close()
    intercept[java.io.IOException] { store.createFile("artifact.jar") }
    val paths = Files.walk(root)
    try {
      import scala.jdk.CollectionConverters._
      paths.iterator().asScala.toList.reverse.foreach(path => Files.delete(path))
    } finally paths.close()
  }
}
