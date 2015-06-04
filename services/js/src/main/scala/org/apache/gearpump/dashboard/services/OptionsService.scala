package org.apache.gearpump.dashboard.services

import com.greencatsoft.angularjs.core.Location
import com.greencatsoft.angularjs.{Factory, Service, injectable}

import scala.scalajs.js.annotation.{JSExport, JSExportAll}

@JSExportAll
case class Conf(updateChartInterval: Int, updateVisDagInterval: Int,
                restapiAutoRefreshInterval: Int, restapiRoot: String,
                webSocketPreferred: Boolean, webSocketSendTimeout: Int)

@JSExport
@injectable("OptionsService")
class OptionsService(location: Location) extends Service {
  val conf = Conf(2000, 2000, 2000, "/api/v1.0", webSocketPreferred = false, 500)
}

@JSExport
@injectable("OptionsService")
class OptionsServiceFactory(location: Location) extends Factory[OptionsService] {

  println("OptionsServiceFactory")

  override def apply() = {
    new OptionsService(location)
  }
}

