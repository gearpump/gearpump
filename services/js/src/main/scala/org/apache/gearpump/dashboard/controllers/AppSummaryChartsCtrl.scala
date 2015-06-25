package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.Interval
import com.greencatsoft.angularjs.{AbstractController, injectable}
import org.apache.gearpump.dashboard.services.ConfService

import scala.scalajs.js
import scala.scalajs.js.undefined
import scala.scalajs.js.annotation.JSExport


@JSExport
@injectable("AppSummaryChartsCtrl")
class AppSummaryChartsCtrl(scope: AppMasterScope, interval: Interval, conf: ConfService)
  extends AbstractController[AppMasterScope](scope) {

  println("AppSummaryChartsCtrl")

  var options = Options(height="108px")
  scope.charts = js.Array(
    Chart(title="Sink Processors Receive Throughput (unit: msgs/s)", options=options, data=js.Array[Double]()),
    Chart(title="Source Processors Send Throughput (unit: msgs/s", options=options, data=js.Array[Double]()),
    Chart(title="Average Processing Time per Task (Unit: ms)", options=options, data=js.Array[Double]()),
    Chart(title="Average Receive Latency per Task (Unit: ms)", options=options, data=js.Array[Double]())
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
        scope.charts(0).data.push(streamingDag.getReceivedMessages(undefined).rate)
        scope.charts(1).data.push(streamingDag.getSentMessages(undefined).rate)
        scope.charts(2).data.push(streamingDag.getProcessingTime(undefined)(0))
        scope.charts(3).data.push(streamingDag.getReceiveLatency(undefined)(0))
      case false =>
    }
  }

  val timeoutPromise = interval(summary _, conf.conf.updateChartInterval)

  scope.$on("$destroy", () => {
    interval.cancel(timeoutPromise)
  })

}
