package org.apache.gearpump.client
/**
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

import akka.actor.{Props, ActorSystem}
import org.apache.gearpump._
import org.apache.gearpump.service.SimpleKVService

class Client {

  def start() : Unit = {
    val appMasterURL = SimpleKVService.get("appMaster")

    val system = ActorSystem("worker", Configs.SYSTEM_DEFAULT_CONFIG)

    val appMaster = system.actorSelection(appMasterURL)
    val app = createApplication()

    appMaster ! SubmitApplication(app)
  }

  def createApplication() : AppDescription = {
    val config = Map()
    val partitioner = new HashPartitioner()
    val split = TaskDescription(Props(classOf[Split], config, partitioner))
    val sum = TaskDescription(Props(classOf[Sum], config, partitioner))
    val app = AppDescription("wordCount", Array(StageDescription(split, 1), StageDescription(sum, 1)))

    app

  }
}

object Client {
  def main(args: Array[String]) {

    val kvService = args(0);
    SimpleKVService.init(kvService)

    new Client().start();
  }
}
