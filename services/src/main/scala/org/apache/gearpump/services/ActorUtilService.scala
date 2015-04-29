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

package org.apache.gearpump.services

import akka.actor.{ActorRef, ActorSystem}
import org.apache.gearpump.util.{Constants, LogUtil}
import spray.routing.HttpService

import scala.concurrent.ExecutionContext

/**
 * A fast util to send some message to specifial actor in the cluster
 */
trait ActorUtilService extends HttpService  {
  import upickle._
  def master:ActorRef
  implicit val system: ActorSystem
  private val LOG = LogUtil.getLogger(getClass)

  def actorUtilRoute = {
    implicit val ec: ExecutionContext = actorRefFactory.dispatcher
    implicit val timeout = Constants.FUTURE_TIMEOUT
    pathPrefix("api"/s"$REST_VERSION") {

      // this is a internal API
      path("internal" / "actorutil") {
        get {
          parameterMap { params =>
            val actorPath = params("actor")
            val actor = system.actorSelection(actorPath)
            val clazz = params("class")
            val msg = Thread.currentThread().getContextClassLoader.loadClass(clazz).newInstance()
            LOG.info(s"forward message ${params("class")} to actor ${actor.anchorPath}")
            actor ! msg

            val response = s"Message of type $clazz is sent to actor $actorPath"
            complete(response)
          }
        }
      }
    }
  }
}