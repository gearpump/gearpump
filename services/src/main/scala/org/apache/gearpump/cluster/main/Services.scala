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

package org.apache.gearpump.cluster.main

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.gearpump.cluster.main.Local._
import org.apache.gearpump.cluster.master.MasterProxy
import org.apache.gearpump.cluster.{ClusterConfig, UserConfig}
import org.apache.gearpump.services.{RestServices,WebSocketServices}
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.LogUtil.ProcessType
import org.apache.gearpump.util.{LogUtil, Util}
import org.slf4j.Logger

object Services extends App with ArgumentsParser {
  val systemConfig = ClusterConfig.load.master.withFallback(ClusterConfig.load.worker)

  private val LOG: Logger = {
    LogUtil.loadConfiguration(systemConfig, ProcessType.UI)
    LogUtil.getLogger(getClass)
  }

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true)
  )

  def start(): Unit = {
    Option(parse(args)).map(options => {
      Option(options.getString("master")).map(masterList => {
        LOG.info("Master URL: " + masterList)

        implicit val system = ActorSystem("services", ClusterConfig.load.application)
        val master = system.actorOf(MasterProxy.props(Util.parseHostList(masterList)), MASTER)
        val config = system.settings.config

        WebSocketServices(master, config)
        RestServices(master, config)
      })
    }
    )
  }

  private[this] def getProp(value: Option[String], text: String): Option[String] = {
    value match {
      case Some(prop) =>
        Option(String.format(text, prop))
      case None =>
        None
    }

  }

  private[this] def getOption(name: String, config: ParseResult): Option[String] = {
    config.exists(name) match {
      case true =>
        val value = config.getString(name)
        Some(value)
      case false =>
        None
    }
  }

  start()
}
