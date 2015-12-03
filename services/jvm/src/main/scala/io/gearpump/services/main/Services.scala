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

package io.gearpump.services.main

import akka.actor.ActorSystem
import com.typesafe.config.ConfigValueFactory
import io.gearpump.cluster.ClusterConfig
import io.gearpump.cluster.main.{CLIOption, ArgumentsParser}
import io.gearpump.cluster.master.MasterProxy
import io.gearpump.services.RestServices
import io.gearpump.util.LogUtil.ProcessType
import io.gearpump.util.{AkkaApp, Constants, LogUtil, Util}
import org.slf4j.Logger

object Services extends AkkaApp with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption("<host:port>", required = false))

  override val description = "UI Server"

  override def akkaConfig: Config = {
    ClusterConfig.load.ui
  }

  override def help: Unit = {
    Console.println("UI Server")
  }

  override def main(inputAkkaConf: Config, args: Array[String]): Unit = {
    val LOG: Logger = {
      LogUtil.loadConfiguration(inputAkkaConf, ProcessType.UI)
      LogUtil.getLogger(getClass)
    }

    val argConfig = parse(args)

    var akkaConf = inputAkkaConf

    import scala.collection.JavaConversions._
    if (argConfig.exists("master")) {
      val master = argConfig.getString("master")
      akkaConf = akkaConf.withValue(Constants.GEARPUMP_CLUSTER_MASTERS,
        ConfigValueFactory.fromIterable(List(master)))
    }

    val masterCluster = akkaConf.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).toList.flatMap(Util.parseHostList)

    implicit val system = ActorSystem("services" , akkaConf)
    import scala.concurrent.duration._
    val master = system.actorOf(MasterProxy.props(masterCluster, 1 day), s"masterproxy${system.name}")

    RestServices(master)

    system.awaitTermination()
  }
}