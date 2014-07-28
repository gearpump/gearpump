package org.apache.gears.cluster

import akka.actor.{Props, ActorSystem}
import org.apache.gearpump.ActorUtil
import org.apache.gearpump.service.SimpleKVService
import org.apache.gearpump.util.ActorSystemBooter
import org.slf4j.{LoggerFactory, Logger}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

class LocalCluster {
  private var system : ActorSystem = null
  private val LOG: Logger = LoggerFactory.getLogger(classOf[LocalCluster])

  def start(kvService : String, workerCount: Int) = {
    SimpleKVService.init(kvService)
    system = ActorSystem("cluster", Configs.SYSTEM_DEFAULT_CONFIG)

    val master = system.actorOf(Props[Master], "master")
    LOG.info("master is started...")

    val masterPath = ActorUtil.getSystemPath(system) + "/user/master"
    SimpleKVService.set("master", masterPath)

    //We are free
    0.until(workerCount).foreach(id => ActorSystemBooter.create.boot(classOf[Worker].getSimpleName + id , masterPath))
    this
  }

  def awaitTermination = {
    system.awaitTermination()
  }
}

object LocalCluster {
  def create = new LocalCluster
}