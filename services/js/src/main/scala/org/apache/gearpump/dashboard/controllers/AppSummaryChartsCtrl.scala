package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.Interval
import com.greencatsoft.angularjs.{AbstractController, injectable}
import org.apache.gearpump.dashboard.services.ConfService

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport


@JSExport
@injectable("AppSummaryChartsCtrl")
class AppSummaryChartsCtrl(scope: AppMasterScope, interval: Interval, conf: ConfService)
  extends AbstractController[AppMasterScope](scope) {

  var options = Options(height="108px")

  scope.charts = js.Array(
    Chart(title="Sink Processors Receive Throughput (unit: msgs/s)", options=options, data=js.Array[Int]()),
    Chart(title="Source Processors Send Throughput (unit: msgs/s", options=options, data=js.Array[Int]()),
    Chart(title="Average Processing Time per Task (Unit: ms)", options=options, data=js.Array[Int]()),
    Chart(title="Average Receive Latency per Task (Unit: ms)", options=options, data=js.Array[Int]())
  )
  println("AppSummaryChartsCtrl")

  var timeoutPromise = interval(() => {
    scope.streamingDag == null || !scope.streamingDag.hasMetrics match {
      case true =>
      case false =>
        scope.charts(0).data += scope.streamingDag.getReceivedMessages(null).rate
        scope.charts(1).data += scope.streamingDag.getSentMessages(null).rate
        scope.charts(2).data += scope.streamingDag.getProcessingTime(null)
        scope.charts(3).data += scope.streamingDag.getReceiveLatency(null)
    }
  }, conf.conf.updateChartInterval)

  scope.$on("$destroy", () => {
    interval.cancel(timeoutPromise)
  })

}
