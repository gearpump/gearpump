/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.gearpump.cluster.main

import org.apache.gearpump.util.{AkkaApp, LogUtil}
import org.slf4j.Logger

/** Tool to run any main class by providing a jar */
object MainRunner extends AkkaApp with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)

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