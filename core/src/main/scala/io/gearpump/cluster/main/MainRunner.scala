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

package io.gearpump.cluster.main

import io.gearpump.util.MasterClientCommand

/** Tool to run any main class by providing a jar */
object MainRunner extends MasterClientCommand with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array(
    // For document purpose only, OPTION_CONFIG option is not used here.
    // OPTION_CONFIG is parsed by parent shell command "Gear" transparently.
    Gear.OPTION_CONFIG -> CLIOption("custom configuration file", required = false,
      defaultValue = None))

  def main(akkaConf: Config, args: Array[String]): Unit = {
    val mainClazz = args(0)
    val commandArgs = args.drop(1)

    val clazz = Thread.currentThread().getContextClassLoader().loadClass(mainClazz)
    val mainMethod = clazz.getMethod("main", classOf[Array[String]])
    mainMethod.invoke(null, commandArgs)
  }
}