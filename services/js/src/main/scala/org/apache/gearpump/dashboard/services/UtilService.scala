package org.apache.gearpump.dashboard.services

import com.greencatsoft.angularjs.{Service, Factory, injectable}

import scala.scalajs.js
import scala.scalajs.js.UndefOr
import scala.scalajs.js.annotation.JSExport

@JSExport
@injectable("UtilService")
class UtilService() extends Service {
  def stringToDateTime(s: UndefOr[Any]): String = {
    s.toOption match {
      case Some(any) =>
        val date = new js.Date(any.toString.toDouble)
        date.formatted("YYYY/MM/DD HH:mm:ss")
      case None =>
        "-"
    }
  }
}

@JSExport
@injectable("UtilService")
class UtilServiceFactory() extends Factory[UtilService] {

  println("UtilServiceFactory")

  override def apply() = {
    new UtilService()
  }
}
