package org.apache.gearpump.services

import akka.actor.ActorRef
import akka.util.Timeout
import scala.concurrent.duration._
import org.apache.gearpump.services.RegistrationActor.{RegisterCheck, NotRegistered, Registered, Register}
import spray.httpx.SprayJsonSupport
import spray.httpx.marshalling.MetaMarshallers
import spray.json._
import spray.routing.Directives
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag


class RegistrationService(registration: ActorRef)(implicit executionContext: ExecutionContext) extends Directives with DefaultJsonProtocol with SprayJsonSupport with MetaMarshallers {
  import akka.pattern.ask

  def jsonObjectFormat[A : ClassTag]: RootJsonFormat[A] = new RootJsonFormat[A] {
    val ct = implicitly[ClassTag[A]]
    def write(obj: A): JsValue = JsObject("value" -> JsString(ct.runtimeClass.getSimpleName))
    def read(json: JsValue): A = ct.runtimeClass.newInstance().asInstanceOf[A]
  }

  implicit val timeout = Timeout(2.seconds)
  implicit val registerFormat = jsonFormat2(Register)
  implicit val registeredFormat = jsonFormat1(Registered)
  implicit val notRegisteredFormat = jsonObjectFormat[NotRegistered.type]


  val route =
    path("master") {
      post {
        parameters('key.as[String].?) {
          (key: Option[String]) =>
            handleWith {
              ru:Register => (registration ? ru).mapTo[Either[NotRegistered.type, Registered]]
            }
        }
      } ~
      get {
        complete((registration ? RegisterCheck("master")).mapTo[Either[NotRegistered.type, Registered]])
      }
    }
}



