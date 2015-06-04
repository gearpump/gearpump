package org.apache.gearpump.services

import akka.actor.ActorRef
import akka.pattern.ask
import org.apache.gearpump.cluster.AppMasterToMaster.GetMasterData
import org.apache.gearpump.shared.Messages.MasterData
import org.apache.gearpump.util.Constants
import spray.http.StatusCodes
import spray.routing
import spray.routing.HttpService

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

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
