package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.{Promise, Interval}
import com.greencatsoft.angularjs.{AbstractController, injectable}
import org.apache.gearpump.dashboard.services.ConfService

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport

@JSExport
@injectable("AppProcessorCtrl")
class AppProcessorCtrl(scope: AppMasterScope, interval: Interval, conf: ConfService)
  extends AbstractController[AppMasterScope](scope) {
  import js.JSConverters._

  scope.$watch("streamingDag", init _)
  scope.$on("$destroy", destroy _)

  var timeoutPromise: Promise = _

  def available: js.Array[String] = {
    (0 to scope.processor.parallelism).map("T"+_).toJSArray
  }

  def formatter(params: js.Array[SkewParams]): String = {
    s"<strong>${params.head.name}</strong><br/>${params.head.data} msgs/s"
  }

  @JSExport
  def init(): Unit = {
    val streamingDag = scope.streamingDag.get
    val activeProcessorId = scope.activeProcessorId.toOption.get
    scope.processor = streamingDag.processors(activeProcessorId)
    scope.processorConnections = streamingDag.calculateProcessorConnections(activeProcessorId)
    val skewData = streamingDag.getReceivedMessages(activeProcessorId).rate
    scope.tasks = js.Dynamic.literal(
      available = available _,
      selected = js.Array[String]()
    ).asInstanceOf[Tasks]
    val dataZoom = js.Dynamic.literal(
      show=true,
      realtime=true,
      y=0,
      height=20
    ).asInstanceOf[SkewDataZoom]
    val formatterMethod = js.Dynamic.literal(
      apply=formatter _
    ).asInstanceOf[js.Function1[js.Array[SkewParams], String]]
    val tooltip = js.Dynamic.literal(
      formatter=formatterMethod
    ).asInstanceOf[SkewToolTip]
    val skewSeriesData = js.Dynamic.literal(
      name="s1",
      data=skewData,
      typeName="bar",
      clickable=true
    ).asInstanceOf[SkewSeriesData]
    val inject=js.Dynamic.literal(
      height="110px",
      xAxisDataNum=scope.processor.parallelism,
      dataZoom=dataZoom,
      tooltip=tooltip
    ).asInstanceOf[SkewInject]
    val options = js.Dynamic.literal(
      inject=inject,
      series=js.Array[SkewSeriesData](skewSeriesData),
      xAxisData=scope.tasks.available()
    ).asInstanceOf[SkewOptions]
    scope.receiveSkewChart = js.Dynamic.literal(
      options=options
    ).asInstanceOf[SkewChart]
    timeoutPromise = interval(fetch _, conf.conf.updateChartInterval)
  }

  def fetch(): Unit = {
    val streamingDag = scope.streamingDag.get
    val receivedMessages = streamingDag.getProcessedMessagesByProcessor(
      streamingDag.meter,
      "receiveThroughput",
      scope.activeProcessorId.get,
      aggregated=false
    ).right.get
    val skewData = receivedMessages.rate
    val data = (0 to scope.processor.parallelism).map(i => {
      js.Dynamic.literal(
        x="t"+i,
        y=skewData(i)
      ).asInstanceOf[SkewData]
    }).toJSArray
    scope.receiveSkewChart.data = data
  }

  def destroy: Unit = {
    interval.cancel(timeoutPromise)
  }

}
