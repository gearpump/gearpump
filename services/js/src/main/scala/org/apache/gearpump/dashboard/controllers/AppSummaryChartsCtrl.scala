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

  println("AppSummaryChartsCtrl")

  var options = Options(height="108px")
  scope.charts = js.Array(
    Chart(title="Sink Processors Receive Throughput (unit: msgs/s)", options=options, data=js.Array[Int]()),
    Chart(title="Source Processors Send Throughput (unit: msgs/s", options=options, data=js.Array[Int]()),
    Chart(title="Average Processing Time per Task (Unit: ms)", options=options, data=js.Array[Int]()),
    Chart(title="Average Receive Latency per Task (Unit: ms)", options=options, data=js.Array[Int]())
  )

  def summary: Unit = {
    val maybeValue = scope.dynamic.$parent.asInstanceOf[AppMasterScope]
    val maybeValue2 = maybeValue.dynamic.$parent.asInstanceOf[AppMasterScope]
    val streamingDag = maybeValue2.streamingDag
    streamingDag.hasMetrics match {
      case true =>
        scope.charts(0).data :+ streamingDag.getReceivedMessages(null).rate
        scope.charts(1).data :+ streamingDag.getSentMessages(null).rate
        scope.charts(2).data :+ streamingDag.getProcessingTime(null)
        scope.charts(3).data :+ streamingDag.getReceiveLatency(null)
      case false =>
    }
  }

  val timeoutPromise = interval(summary _, conf.conf.updateChartInterval)

  scope.$on("$destroy", () => {
    interval.cancel(timeoutPromise)
  })

}
