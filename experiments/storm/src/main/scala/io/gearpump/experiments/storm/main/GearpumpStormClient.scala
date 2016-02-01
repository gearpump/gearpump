/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.experiments.storm.main

import backtype.storm.Config
import backtype.storm.utils.Utils
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import io.gearpump.util.Constants._
import io.gearpump.util.{AkkaApp, LogUtil, Util}

object GearpumpStormClient extends AkkaApp with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array(
    "jar" -> CLIOption[String]("<storm jar>", required = true),
    "config" -> CLIOption[Int]("<storm config file>", required = true),
    "verbose" -> CLIOption("<print verbose log on console>", required = false, defaultValue = Some(false)))

  override def main(inputAkkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)

    val verbose = config.getBoolean("verbose")
    if (verbose) {
      LogUtil.verboseLogToConsole
    }

    val jar = config.getString("jar")
    val stormConfig = config.getString("config")
    val topology = config.remainArgs(0)
    val stormArgs = config.remainArgs.drop(1)
    val stormOptions = Array(
      s"-Dstorm.options=${getThriftOptions(stormConfig)}",
      s"-Dstorm.jar=$jar",
      s"-Dstorm.config.file=$stormConfig",
      s"-D${PREFER_IPV4}=true"
    )

    val classPath = Array(s"${System.getProperty(GEARPUMP_HOME)}/lib/storm/*", jar)
    val process = Util.startProcess(stormOptions, classPath, topology, stormArgs)

    // wait till the process exit
    val exit = process.exitValue()

    if (exit != 0) {
      throw new Exception(s"failed to submit jar, exit code $exit, error summary: ${process.logger.error}")
    }
  }

  private def getThriftOptions(stormConfig: String): String = {
    val config = Utils.findAndReadConfigFile(stormConfig, true)
    val host = config.get(Config.NIMBUS_HOST)
    val thriftPort = config.get(Config.NIMBUS_THRIFT_PORT)
    s"${Config.NIMBUS_HOST}=$host,${Config.NIMBUS_THRIFT_PORT}=$thriftPort"
  }
}
