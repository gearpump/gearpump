package org.apache.gearpump.dashboard.services

import com.greencatsoft.angularjs.core.Location
import com.greencatsoft.angularjs.{Factory, Service, injectable}

import scala.scalajs.js.annotation.{JSExport, JSExportAll}

@JSExportAll
case class Conf(updateChartInterval: Int, updateVisDagInterval: Int, updateMetricsInterval: Int,
                restapiAutoRefreshInterval: Int, restapiRoot: String,
                webSocketPreferred: Boolean, webSocketSendTimeout: Int)

@JSExport
@injectable("ConfService")
class ConfService(location: Location) extends Service {
  val conf = Conf(
    updateChartInterval=2000,
    updateVisDagInterval=2000,
    updateMetricsInterval=1000,
    restapiAutoRefreshInterval=2000,
    restapiRoot="/api/v1.0",
    webSocketPreferred=false,
    webSocketSendTimeout=500
  )
}

@JSExport
@injectable("ConfService")
class ConfServiceFactory(location: Location) extends Factory[ConfService] {
  override def apply() = {
    new ConfService(location)
  }
}

