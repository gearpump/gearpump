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

import akka.actor.{ActorSystem, Props}
import org.apache.gearpump.cluster.ClusterConfig
import org.apache.gearpump.cluster.master.MasterProxy
import org.apache.gearpump.cluster.worker.{Worker => WorkerActor}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.LogUtil.ProcessType
import org.apache.gearpump.util.{AkkaApp, LogUtil}
import org.slf4j.Logger

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/** Tool to start a worker daemon process */
object Worker extends AkkaApp with ArgumentsParser {
  protected override def akkaConfig = ClusterConfig.worker()

  override val description = "Start a worker daemon"

  var LOG: Logger = LogUtil.getLogger(getClass)

  private def uuid = java.util.UUID.randomUUID.toString

  def main(akkaConf: Config, args: Array[String]): Unit = {
    val id = uuid

    this.LOG = {
      LogUtil.loadConfiguration(akkaConf, ProcessType.WORKER)
      // Delay creation of LOG instance to avoid creating an empty log file as we
      // reset the log file name here
      LogUtil.getLogger(getClass)
    }

    val system = ActorSystem(id, akkaConf)

    val masterAddress = akkaConf.getStringList(GEARPUMP_CLUSTER_MASTERS).asScala.map { address =>
      val hostAndPort = address.split(":")
      HostPort(hostAndPort(0), hostAndPort(1).toInt)
    }

    LOG.info(s"Trying to connect to masters " + masterAddress.mkString(",") + "...")
    val masterProxy = system.actorOf(MasterProxy.props(masterAddress), s"masterproxy${system.name}")

    system.actorOf(Props(classOf[WorkerActor], masterProxy),
      classOf[WorkerActor].getSimpleName + id)

    Await.result(system.whenTerminated, Duration.Inf)
  }
}