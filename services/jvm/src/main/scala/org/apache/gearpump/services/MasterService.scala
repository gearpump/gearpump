package org.apache.gearpump.services

import akka.actor.ActorRef
import org.apache.gearpump.cluster.AppMasterToMaster.{MasterData, GetMasterData}
import org.apache.gearpump.util.{Constants}
import spray.http.StatusCodes
import spray.routing
import spray.routing.HttpService

import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Failure, Success}
import akka.pattern.ask

trait MasterService extends HttpService {
  import upickle._
  def master:ActorRef

  def masterRoute: routing.Route = {
    implicit val ec: ExecutionContext = actorRefFactory.dispatcher
    implicit val timeout = Constants.FUTURE_TIMEOUT
    pathPrefix("api"/s"$REST_VERSION") {
      path("master") {
        get {
          onComplete((master ? GetMasterData).asInstanceOf[Future[MasterData]]) {
            case Success(value: MasterData) => complete(write(value))
            case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
          }
        }
      }
    }
  }
}
