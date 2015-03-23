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
import org.apache.gearpump.cluster.ClusterConfig
import org.apache.gearpump.cluster.master.MasterProxy
import org.apache.gearpump.cluster.worker.{Worker => WorkerActor}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.LogUtil
import org.apache.gearpump.util.LogUtil.ProcessType
import org.slf4j.Logger

import scala.collection.JavaConverters._

object
Worker extends App with ArgumentsParser {
  val config = ClusterConfig.load.worker
  val LOG : Logger = {
    LogUtil.loadConfiguration(config, ProcessType.WORKER)
    //delay creation of LOG instance to avoid creating an empty log file as we reset the log file name here
    LogUtil.getLogger(getClass)
  }

  def uuid = java.util.UUID.randomUUID.toString

  val options: Array[(String, CLIOption[Any])] =
    Array("master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = false))

  def start(): Unit = {
    worker()
  }

  def worker(): Unit = {
    val id = uuid
    val system = ActorSystem(id, config)
    val mastersAddresses:Option[Iterable[HostPort]] = getMasterAddressFromArgs orElse getMastersAddressesFromConfig
    mastersAddresses match {
      case Some(addresses) =>
        LOG.info(s"Trying to connect to masters " + addresses.mkString(",") + "...")
        val masterProxy = system.actorOf(MasterProxy.props(addresses), MASTER)
        system.actorOf(Props(classOf[WorkerActor], masterProxy),
          classOf[WorkerActor].getSimpleName + id)
        system.awaitTermination()
      case None =>
        LOG.error("Master address not found as command line option or in conf")
        system.shutdown()
    }
  }

  def getMasterAddressFromArgs:Option[Iterable[HostPort]] = {
       val cmdLineConfig = Option(parse(args))
       cmdLineConfig match {
         case Some(opts) =>
           opts.exists("master") match {
             case true =>
               Some(opts.getString("master").split(",").map(hostAndPort))
             case false =>
               None
           }
         case None =>
           None
       }
  }
  
  def getMastersAddressesFromConfig:Option[Iterable[HostPort]] = {
    Some(config.getStringList("gearpump.cluster.masters").asScala.map(hostAndPort))
  }

  private[this] def hostAndPort(address: String): HostPort = {
    val hostAndPort = address.split(":")
    HostPort(hostAndPort(0), hostAndPort(1).toInt)
  }

  start()
}
