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


package org.apache.gearpump.streaming.examples.stock

import java.util.concurrent.TimeUnit

import akka.actor.{Props, Actor}
import akka.io.IO
import org.apache.gearpump.streaming.appmaster.AppMaster.{TaskActorRef, LookupTaskActorRef}
import akka.actor.Actor._
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.MasterToAppMaster.AppMasterDataDetailRequest
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.examples.stock.QueryServer.WebServer
import org.apache.gearpump.streaming.{ProcessorId, ProcessorDescription}
import org.apache.gearpump.streaming.appmaster.{ProcessorSummary, StreamAppMasterSummary}
import org.apache.gearpump.streaming.task.{TaskId, StartTime, Task, TaskContext}
import akka.pattern.ask
import spray.can.Http
import spray.http.{StatusCodes}
import spray.routing.HttpService
import upickle.default.{read, write}
import spray.json._

import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Failure, Success}

class QueryServer(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf){
  import taskContext.{appMaster, appId}

  import ExecutionContext.Implicits.global

  var analyzer: (ProcessorId, ProcessorSummary) = null
  implicit val timeOut = akka.util.Timeout(3, TimeUnit.SECONDS)

  override def onStart(startTime: StartTime): Unit = {
    appMaster ! AppMasterDataDetailRequest(appId)
    taskContext.actorOf(Props(new WebServer))
  }

  override def onNext(msg: Message): Unit = {
   //Skip
  }

  override def receiveUnManagedMessage: Receive = messageHandler

  def messageHandler: Receive = {
    case detail: StreamAppMasterSummary =>
      analyzer = detail.processors.find { kv =>
        val (processorId, processor) = kv
        processor.taskClass == classOf[Analyzer].getName
      }.get
    case getReport @ GetReport(stockId, date) =>
      val parallism = analyzer._2.parallelism
      val processorId = analyzer._1
      val analyzerTaskId = TaskId(processorId, (stockId.hashCode & Integer.MAX_VALUE) % parallism)
      val requester = sender
      import scala.concurrent.Future
      (appMaster ? LookupTaskActorRef(analyzerTaskId))
        .asInstanceOf[Future[TaskActorRef]].flatMap {task =>

        (task.task ? getReport).asInstanceOf[Future[Report]]
      }.map { report =>
        LOG.info(s"reporting $report")
        requester ! report
      }
    case _ =>
    //ignore
  }
}

object QueryServer {
  class WebServer extends Actor with HttpService {

    import context.dispatcher
    implicit val timeOut = akka.util.Timeout(3, TimeUnit.SECONDS)
    def actorRefFactory = context
    implicit val system = context.system

    IO(Http) ! Http.Bind(self, interface = "localhost", port = 8080)

    override def receive: Receive = runRoute(webServer ~ staticRoute)

    def webServer = {
      path("report" / PathElement) { stockId =>
        get {
          onComplete((context.parent ? GetReport(stockId, null)).asInstanceOf[Future[Report]]) {
            case Success(report: Report) =>
              val json = write(report)
              complete(pretty(json))
            case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
          }
        }
      }
    }

    val staticRoute = {
      pathEndOrSingleSlash {
        getFromResource("stock/stock.html")
      } ~
      pathPrefix("css") {
        get {
          getFromResourceDirectory("stock/css")
        }
      } ~
      pathPrefix("js") {
        get {
          getFromResourceDirectory("stock/js")
        }
      }
    }

    private def pretty(json: String): String = {
      json.parseJson.prettyPrint
    }
  }
}
