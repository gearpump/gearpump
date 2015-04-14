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
package org.apache.gearpump.examples.transport

import java.util.concurrent.TimeUnit

import akka.actor.{Props, Actor}
import akka.actor.Actor._
import akka.io.IO
import akka.pattern.ask
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.examples.transport.QueryServer.{GetAllRecords, WebServer}
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming._
import org.apache.gearpump.streaming.appmaster.AppMaster.{TaskActorRef, LookupTaskActorRef}
import org.apache.gearpump.streaming.task.{TaskId, StartTime, Task, TaskContext}
import org.apache.gearpump.util.Graph
import spray.can.Http
import spray.http.StatusCodes
import spray.routing.HttpService
import upickle._
import spray.json._

import scala.concurrent.Future
import scala.util.{Failure, Success}

class QueryServer(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf){
  import taskContext.{appMaster, appId}
  import system.dispatcher

  var inspector: (ProcessorId, ProcessorDescription) = null
  implicit val timeOut = akka.util.Timeout(3, TimeUnit.SECONDS)
  private var overSpeedRecords = List.empty[OverSpeedReport]

  override def onStart(startTime: StartTime): Unit = {
    val dag = DAG(conf.getValue[Graph[ProcessorDescription, Partitioner]](StreamApplication.DAG).get)
    inspector = dag.processors.find { kv =>
      val (_, processor) = kv
      processor.taskClass == classOf[VelocityInspector].getName
    }.get
    taskContext.actorOf(Props(new WebServer))
  }

  override def onNext(msg: Message): Unit = {
  }

  override def receiveUnManagedMessage: Receive = {
    case getTrace @ GetTrace(vehicleId: String) =>
      val parallism = inspector._2.parallelism
      val processorId = inspector._1
      val analyzerTaskId = TaskId(processorId, (vehicleId.hashCode & Integer.MAX_VALUE) % parallism)
      val requester = sender
      (appMaster ? LookupTaskActorRef(analyzerTaskId))
        .asInstanceOf[Future[TaskActorRef]].flatMap { task =>
        (task.task ? getTrace).asInstanceOf[Future[VehicleTrace]]
      }.map { trace =>
        LOG.info(s"reporting $trace")
        requester ! trace
      }
    case record@ OverSpeedReport(vehicleId, speed, timestamp, locationId) =>
      LOG.info(s"vehicle $vehicleId is over speed, the speed is $speed km/h")
      overSpeedRecords :+= record
    case GetAllRecords =>
      sender ! QueryServer.OverSpeedRecords(overSpeedRecords.toArray.sortBy(_.timestamp))
      overSpeedRecords = List.empty[OverSpeedReport]
    case _ =>
      //ignore
  }
}

object QueryServer {
  object GetAllRecords

  case class OverSpeedRecords(records: Array[OverSpeedReport])

  class WebServer extends Actor with HttpService {

    import context.dispatcher
    implicit val timeOut = akka.util.Timeout(3, TimeUnit.SECONDS)
    def actorRefFactory = context
    implicit val system = context.system

    IO(Http) ! Http.Bind(self, interface = "localhost", port = 8080)

    override def receive: Receive = runRoute(webServer ~ staticRoute)

    def webServer = {
      path("trace" / PathElement) { vehicleId =>
        get {
          onComplete((context.parent ? GetTrace(vehicleId)).asInstanceOf[Future[VehicleTrace]]) {
            case Success(trace: VehicleTrace) =>
              val json = write(trace)
              complete(pretty(json))
            case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
          }
        }
      } ~
      path("records") {
        get {
          onComplete((context.parent ? GetAllRecords).asInstanceOf[Future[OverSpeedRecords]]) {
            case Success(records: OverSpeedRecords) =>
              val json = write(records)
              complete(pretty(json))
            case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
          }
        }
      }
    }

    val staticRoute = {
      pathEndOrSingleSlash {
        getFromResource("transport/transport.html")
      } ~
        pathPrefix("css") {
          get {
            getFromResourceDirectory("transport/css")
          }
        } ~
        pathPrefix("svg") {
          get {
            getFromResourceDirectory("transport/svg")
          }
        } ~
        pathPrefix("js") {
          get {
            getFromResourceDirectory("transport/js")
          }
        }
    }

    private def pretty(json: String): String = {
      json.parseJson.prettyPrint
    }
  }
}
