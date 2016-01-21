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

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer
import io.gearpump.cluster.ClientToMaster._
import io.gearpump.util.ActorUtil._

import scala.util.{Failure, Success}


class SupervisorService(val supervisor: ActorRef, override val system: ActorSystem)
  extends BasicService {

  import upickle.default.write

  override def doRoute(implicit mat: Materializer) = pathPrefix("supervisor") {
    pathEnd {
      get {
        complete(write(supervisor.path.toString))
      }
    } ~
    path("addworker" / IntNumber) { count =>
      post {
        onComplete(askActor[CommandResult](supervisor, AddWorker(count))) {
          case Success(value) =>
            complete(write(value))
          case Failure(ex) =>
            failWith(ex)
        }
      }
    } ~
      path("addmaster") {
        post {
          onComplete(askActor[CommandResult](supervisor, AddMaster)) {
            case Success(value) =>
              complete(write(value))
            case Failure(ex) =>
              failWith(ex)
          }
        }
      } ~
        path("removemaster" / Segment) { uriPath =>
          post {
            onComplete(askActor[CommandResult](supervisor, RemoveMaster(uriPath))) {
              case Success(value) =>
                complete(write(value))
              case Failure(ex) =>
                failWith(ex)
            }
          }
        } ~
        path("removeworker" / Segment) { uriPath =>
          post {
            onComplete(askActor[CommandResult](supervisor, RemoveWorker(uriPath))) {
              case Success(value) =>
                complete(write(value))
              case Failure(ex) =>
                failWith(ex)
            }
          }
        }
  }
}
