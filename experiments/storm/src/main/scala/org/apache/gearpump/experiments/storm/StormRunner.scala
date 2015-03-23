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

package org.apache.gearpump.experiments.storm

import backtype.storm.Config
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{CLIOption, ArgumentsParser}
import org.apache.gearpump.util.{Constants, Util}


object StormRunner extends App with ArgumentsParser {
  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    "storm_topology" -> CLIOption[String]("<storm topology main class>", required = true),
    "storm_args" -> CLIOption[String]("<storm topology name>", required = false),
    "runseconds"-> CLIOption[Int]("<how long to run this example>", required = false, defaultValue = Some(60)))

  val config = parse(args)

  val clientContext = ClientContext(config.getString("master"))
  val thriftServer = GearpumpThriftServer(clientContext)
  thriftServer.start()

  val topologyClass = config.getString("storm_topology")
  val stormArgs = config.getString("storm_args")
  val stormOptions = Array("-Dstorm.options=" +
    s"${Config.NIMBUS_HOST}=127.0.0.1,${Config.NIMBUS_THRIFT_PORT}=${GearpumpThriftServer.THRIFT_PORT}",
    "-Dstorm.jar=" + System.getProperty(Constants.GEARPUMP_APP_JAR)
  )

  val classPath = Array(System.getProperty("java.class.path"))
  val arguments = stormArgs.split(",")
  Util.startProcess(stormOptions, classPath, topologyClass, arguments)

  Thread.sleep(config.getInt("runseconds") * 1000)
  thriftServer.close()
  clientContext.close()
}
