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

package io.gearpump.services

import akka.actor.{ActorSystem}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.{Materializer}
import io.gearpump.util.{Constants, Util}

/**
 * AdminService is for cluster-wide managements. it is not related with
 * specific application.
 *
 * For example:
 * Security management: Add user, remove user.
 * Configuration management: Change configurations.
 * Machine management: Add worker machines, remove worker machines, and add masters.
 */

// TODO: Add YARN resource manager capacities to add/remove machines.
class AdminService(override val system: ActorSystem)
  extends BasicService {

  override def prefix = Neutral

  override def doRoute(implicit mat: Materializer) = {
    path("terminate") {
      post {
        system.shutdown()
        complete(StatusCodes.NotFound)
      }
    }
  }
}
