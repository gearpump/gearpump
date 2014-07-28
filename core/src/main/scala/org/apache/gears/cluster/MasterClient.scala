package org.apache.gears.cluster

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

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef}
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import akka.pattern.ask
import scala.concurrent.Future

class MasterClient(master : ActorRef) {
  private implicit val timeout = Timeout(5, TimeUnit.SECONDS)

  /**
   * return appId
   * throw if anything wrong
   */
  def submitApplication(appMaster : Class[_ <: Actor], config : Configs, app : Application) : Int = {
    Await.result( (master ? SubmitApplication(appMaster, config, app)).asInstanceOf[Future[Int]], Duration.Inf)
  }

  /**
   * Throw exception if fail to shutdown
   */
  def shutdownApplication(appId : Int) : Unit = {
    Await.result(master ? ShutdownApplication(appId), Duration.Inf)
  }
}