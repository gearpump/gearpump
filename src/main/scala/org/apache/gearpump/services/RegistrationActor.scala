package org.apache.gearpump.services

import akka.actor.Actor
import org.slf4j.{LoggerFactory, Logger}

class RegistrationActor extends Actor {
  private val LOG: Logger = LoggerFactory.getLogger(RegistrationActor.getClass)

  import org.apache.gearpump.services.RegistrationActor._
  private var registry = Map[String, String]()

  def receive: Receive = {
    case RegisterCheck(name) =>
      registry.isDefinedAt(name) match {
        case true =>
          LOG.info("RegisterCheck true")
          sender ! Right(Registered(registry("master")))
        case false =>
          LOG.info("RegisterCheck false")
          sender ! Left(NotRegistered)
      }
    case Register(name, url) if registry.isDefinedAt(name) =>
      sender ! Right(Registered(registry("master")))
    case Register(name:String, url:String)  =>
      registry += name -> url
      sender ! Left(NotRegistered)
  }

}

object RegistrationActor {
  case class Register(name: String, url: String)
  case class RegisterCheck(name: String)
  case class Registered(url: String)
  case object NotRegistered

}

