package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.Interval
import com.greencatsoft.angularjs.{AbstractController, injectable}
import org.apache.gearpump.dashboard.services.{Conf, ConfService}

import scala.scalajs.js
import scala.scalajs.js.undefined
import scala.scalajs.js.annotation.JSExport
import scala.util.{Failure, Success, Try}

@JSExport
@injectable("AppSummaryChartsCtrl")
class AppSummaryChartsCtrl(scope: AppMasterScope, interval: Interval, conf: ConfService)
  extends AbstractController[AppMasterScope](scope) {

  var options = Options(height="108px")
  scope.charts = js.Array(
    Chart(title="Sink Processors Receive Throughput (unit: msgs/s)", options=options, data=js.Array[Double]()),
    Chart(title="Source Processors Send Throughput (unit: msgs/s", options=options, data=js.Array[Double]()),
    Chart(title="Average Processing Time per Task (Unit: ms)", options=options, data=js.Array[Double]()),
    Chart(title="Average Receive Latency per Task (Unit: ms)", options=options, data=js.Array[Double]())
  )

  def fetch(): Unit = {
    Try({
      val streamingDag = scope.streamingDag
      streamingDag.get.hasMetrics match {
        case true =>
          scope.charts(0).data = js.Array(streamingDag.get.getReceivedMessages(undefined).rate)
          scope.charts(1).data = js.Array(streamingDag.get.getSentMessages(undefined).rate)
          scope.charts(2).data = js.Array(streamingDag.get.getProcessingTime(undefined)(0))
          scope.charts(3).data = js.Array(streamingDag.get.getReceiveLatency(undefined)(0))
        case false =>
      }
    }) match {
      case Success(ok) =>
      case Failure(throwable) =>
        println(s"failed ${throwable.getMessage}")
    }
  }

  val timeoutPromise = interval(fetch _, conf.conf.updateChartInterval)

  scope.$on("$destroy", () => {
    interval.cancel(timeoutPromise)
  })

}
