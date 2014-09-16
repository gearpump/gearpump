package org.apache.gearpump.services

import akka.actor.{Actor, ActorSystem, Props}
import akka.io.IO
import com.gettyimages.spray.swagger._
import com.typesafe.config.ConfigFactory
import com.wordnik.swagger.model.ApiInfo
import spray.can._
import spray.json._
import spray.routing.HttpService

import scala.reflect.runtime.universe._

class Services extends Actor with HttpService with DefaultJsonProtocol {

  def actorRefFactory = context

    val appMasterService = new AppMasterService {
      def actorRefFactory = context
    }

    def receive = runRoute(appMasterService.routes ~ swaggerService.routes ~
      get {
        pathPrefix("") { 
          pathEndOrSingleSlash {
            getFromResource("swagger-ui/index.html")
          }
        } ~
        getFromResourceDirectory("swagger-ui")
      })

  val swaggerService = new SwaggerHttpService {
    override def apiTypes = Seq(typeOf[AppMasterService])
    override def apiVersion = "2.0"
    override def baseUrl = "/"
    override def docsPath = "api-docs"
    override def actorRefFactory = context
    override def apiInfo = Some(new ApiInfo("Spray-Swagger", "A service using spray and spray-swagger.", "TOC Url", "Michael Hamrah @mhamrah", "Apache V2", "http://www.apache.org/licenses/LICENSE-2.0"))

    //authorizations, not used
  }
}

object Services {
  def start(implicit system:ActorSystem) {
    implicit val executionContext = system.dispatcher
    val services = system.actorOf(Props(new Services), "services")
    val config = ConfigFactory.load()
    val port = config.getInt("gearpump.services.port")
    val host = config.getString("gearpump.services.host")
    IO(Http) ! Http.Bind(services, interface = host, port = port)
  }
}
