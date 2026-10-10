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

package io.gearpump.util

import com.typesafe.config.ConfigFactory
import java.io.File
import java.util.concurrent.TimeUnit
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JvmConfigSpec extends AnyFlatSpec with Matchers {
  private val config = ConfigFactory.parseResources("distribution-gear.conf")
  private val settings = Util.resolveJvmSetting(config)
  private val javaExecutable = new File(System.getProperty("java.home"),
    if (System.getProperty("os.name").startsWith("Windows")) "bin/java.exe" else "bin/java")

  for ((name, setting) <- Seq("AppMaster" -> settings.appMater, "executor" -> settings.executor)) {
    "Distribution JVM options" should s"start the $name JVM on the current JDK" in {
      setting.vmargs should not be empty
      val command = Seq(javaExecutable.getAbsolutePath) ++ setting.vmargs.toSeq :+ "-version"
      val process = new ProcessBuilder(command: _*).inheritIO().start()
      try {
        assert(process.waitFor(30, TimeUnit.SECONDS), s"$name JVM did not exit")
        process.exitValue() shouldBe 0
      } finally {
        process.destroyForcibly()
      }
    }
  }
}
