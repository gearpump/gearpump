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

import akka.actor.{ActorSystem, ActorRef}
import akka.pattern.ask
import org.apache.gearpump._
import org.apache.gearpump.cluster.AppMasterToMaster.{AppMasterDataDetail}
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMasterDataDetailRequest, AppMasterDataRequest}
import org.apache.gearpump.cluster.{Application, UserConfig}
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.{AppDescription, TaskDescription, DAG}
import org.apache.gearpump.util.{Graph, Constants}
import spray.http.StatusCodes
import spray.routing.HttpService
import upickle.{Js, Writer, Reader}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import AppMasterService._

trait AppMasterService extends HttpService  {
  import upickle._
  def master:ActorRef
  implicit val system: ActorSystem

  def appMasterRoute = get {
    implicit val ec: ExecutionContext = actorRefFactory.dispatcher
    implicit val timeout = Constants.FUTURE_TIMEOUT
    path("appmaster"/IntNumber) { appId => {
       parameter("detail" ? "false") { detail =>
         val detailValue = Try(detail.toBoolean).getOrElse(false)
         detailValue match {
           case true =>
             onComplete((master ? AppMasterDataDetailRequest(appId)).asInstanceOf[Future[AppMasterDataDetail]]) {
               case Success(value: AppMasterDataDetail) =>

                 val streamApp: StreamingAppMasterDataDetail = value
                 complete(write(streamApp))
               case Failure(ex) =>
                 complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
              }
           case false =>
             onComplete((master ? AppMasterDataRequest(appId)).asInstanceOf[Future[AppMasterData]]) {
               case Success(value: AppMasterData) => complete(write(value))
               case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
             }
         }
       }
     }
    }
  }
}

object AppMasterService {
  case class StreamingAppMasterDataDetail(appId: Int, appName: String = null, dag: Graph[TaskDescription, Partitioner] = null,
                                          actorPath: String = null, clock: TimeStamp = 0, executors: List[String] = null)

  // for now we only serialize name and dag. We may also need a reader once we allow DAG mods.
  implicit val writer: Writer[StreamingAppMasterDataDetail] = upickle.Writer[StreamingAppMasterDataDetail] {

    case app => Js.Obj(
      ("appId", Js.Num(app.appId)),
      ("appName", Js.Str(app.appName)),
      ("actorPath", Js.Str(app.actorPath)),
      ("clock", Js.Num(app.clock)),
      ("executors", Js.Arr(app.executors.map(str => Js.Str(str)).toSeq:_*)),
      ("dag", Js.Obj(
        ("vertices", Js.Arr(app.dag.vertices.map(taskDescription => {
          Js.Str(taskDescription.taskClass)
        }).toSeq:_*)),
        ("edges", Js.Arr(app.dag.edges.map(f => {
          var (node1, edge, node2) = f
          Js.Arr(Js.Str(node1.taskClass), Js.Str(edge.getClass.getName), Js.Str(node2.taskClass))
        }).toSeq:_*)))))
  }

  implicit def appMasterDetailToStreaming(app: AppMasterDataDetail)(implicit system: ActorSystem): StreamingAppMasterDataDetail = {
    import app.{appId, appName, application, actorPath, clock, executors}
    import AppDescription._
    val appDescription: AppDescription =  application
    return new StreamingAppMasterDataDetail(appId, appName, appDescription.dag, actorPath, clock, executors)
  }
}
