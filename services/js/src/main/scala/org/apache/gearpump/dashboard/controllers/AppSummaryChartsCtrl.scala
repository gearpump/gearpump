package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.Interval
import com.greencatsoft.angularjs.{AbstractController, injectable}
import org.apache.gearpump.dashboard.services.ConfService

import scala.scalajs.js
import scala.scalajs.js.undefined
import scala.scalajs.js.annotation.JSExport
import scala.util.Try


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

  def fetch: Unit = {
    Try({
      val streamingDag = scope.streamingDag
      streamingDag.hasMetrics match {
        case true =>
          val receivedMessages = streamingDag.getReceivedMessages(undefined).rate
          val sentMessages = streamingDag.getSentMessages(undefined).rate
          println(s"receivedMessages=$receivedMessages sentMessages=$sentMessages charts.size=${scope.charts.size} charts(0).data.size=${scope.charts(0).data.size}")
          scope.charts(0).data.push(receivedMessages)
          scope.charts(1).data.push(sentMessages)
          scope.charts(2).data.push(streamingDag.getProcessingTime(undefined)(0))
          scope.charts(3).data.push(streamingDag.getReceiveLatency(undefined)(0))
        case false =>
          println("no metrics")
      }
    }).failed.foreach(throwable => {
      println(s"failed ${throwable.getMessage}")
    })
  }

  val timeoutPromise = interval(fetch _, conf.conf.updateChartInterval)

  scope.$on("$destroy", () => {
    interval.cancel(timeoutPromise)
  })

}
