package org.apache.gearpump.services

import akka.actor.{Actor, ActorSystem, Props}
import akka.io.IO
import spray.can._
import spray.http.MediaTypes._
import spray.json._
import spray.routing.HttpService

case class Help(usage: String="hi")

class Services extends Actor with HttpService with DefaultJsonProtocol {
  implicit val helpFormat = jsonFormat1(Help)

  def actorRefFactory = context

  def receive = runRoute(serviceRoute)

  val serviceRoute = 
      pathPrefix("api") {
        get {
          respondWithMediaType(`application/json`) {
            complete {
              Help().toJson.prettyPrint
            }
          }
        }
      }
}

object Services {
  def start(implicit system:ActorSystem) {
    implicit val executionContext = system.dispatcher
    val services = system.actorOf(Props(new Services), "services")
    IO(Http) ! Http.Bind(services, interface = "0.0.0.0", port = 8080)
  }
}
