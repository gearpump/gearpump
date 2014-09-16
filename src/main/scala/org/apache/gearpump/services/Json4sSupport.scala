package org.apache.gearpump.services

import spray.httpx.Json4sJacksonSupport
import org.json4s._
import java.util.UUID

object Json4sSupport extends Json4sJacksonSupport {
   implicit def json4sJacksonFormats: Formats = jackson.Serialization.formats(NoTypeHints) + new UUIDFormat

  //so you don't need to import
  //jackson everywhere
  val jsonMethods = org.json4s.jackson.JsonMethods


  class UUIDFormat extends Serializer[UUID] {
    val UUIDClass = classOf[UUID]

    def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), UUID] = {
      case (TypeInfo(UUIDClass, _), JString(x)) => UUID.fromString(x)
    }

    def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case x: UUID => JString(x.toString)
    }
  }

  def toJValue[T](value: T): JValue = {
    Extraction.decompose(value)
  }
}
