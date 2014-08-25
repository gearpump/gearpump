package org.apache.gearpump.services

import akka.actor._
import spray.http.StatusCodes._
import spray.http._
import spray.routing._
import spray.util.LoggingContext

import scala.util.control.NonFatal


class RoutedHttpService(var route: Route) extends Actor with HttpService with ActorLogging {
  case class ErrorResponseException(responseStatus: StatusCode, response: Option[HttpEntity]) extends Exception
  implicit def actorRefFactory = context

  implicit val handler = ExceptionHandler {
    case NonFatal(ErrorResponseException(statusCode, entity)) => ctx =>
      ctx.complete((statusCode, entity))

    case NonFatal(e) => ctx => {
      log.error(e, InternalServerError.defaultMessage)
      ctx.complete(InternalServerError)
    }
  }

  def receive: Receive = runRoute(route)(handler, RejectionHandler.Default, context, RoutingSettings.default, LoggingContext.fromActorRefFactory)

}
