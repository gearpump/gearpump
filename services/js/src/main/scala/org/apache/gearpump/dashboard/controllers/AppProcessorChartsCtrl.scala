package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.{Interval, Promise}
import com.greencatsoft.angularjs.{AbstractController, injectable}
import org.apache.gearpump.dashboard.services.ConfService

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport

@JSExport
@injectable("AppProcessorChartsCtrl")
class AppProcessorChartsCtrl(scope: AppMasterScope, interval: Interval, conf: ConfService)
  extends AbstractController[AppMasterScope](scope) {
  import js.JSConverters._

  var timeoutPromise: Promise = _

  scope.$watch("streamingDag", init _)
  scope.$watchCollection("tasks", charts _)
  scope.$on("$destroy", destroy _)

  @JSExport
  def init(): Unit = {
    timeoutPromise = interval(fetch _, conf.conf.updateChartInterval)
  }

  def formatter(params: js.Array[SkewSeriesParams]): String = {
    val first = s"${params.head.name}<br/>${params.head.seriesName}:${params.head.data}}"
    params.drop(1).foldLeft(first)((name,param) => {
      first + s"${param.name}<br/>${param.seriesName}:${param.data}}"
    })
  }

  def getOptions(tasks: Tasks): SkewOptions = {
    val formatterMethod = js.Dynamic.literal(
      apply=formatter _
    ).asInstanceOf[js.Function1[js.Array[SkewSeriesParams], String]]
    val tooltip = js.Dynamic.literal(
      formatter=formatterMethod
    ).asInstanceOf[SkewToolTip]
    val inject=js.Dynamic.literal(
      height="108px",
      xAxisDataNum=15,
      tooltip=tooltip
    ).asInstanceOf[SkewInject]
    val series = tasks.selected.map(value => {
      js.Dynamic.literal(
        name=value,
        date=js.Array[Double](0),
        scale=true
      ).asInstanceOf[SkewSeriesData]
    }).toJSArray
    js.Dynamic.literal(
      inject=inject,
      series=series,
      xAxisData=js.Array[String]("")
    ).asInstanceOf[SkewOptions]
  }

  def charts(tasks: Tasks): Unit = {
    println("in charts")
    tasks.selected.length > 0 match {
      case true =>
        scope.receiveMessageRateChart = js.Dynamic.literal(
          options=getOptions(tasks)
        ).asInstanceOf[SkewChart]
        scope.sendMessageRateChart = js.Dynamic.literal(
          options=getOptions(tasks)
        ).asInstanceOf[SkewChart]
        scope.processingTimeChart = js.Dynamic.literal(
          options=getOptions(tasks)
        ).asInstanceOf[SkewChart]
        scope.receiveLatencyChart = js.Dynamic.literal(
          options=getOptions(tasks)
        ).asInstanceOf[SkewChart]
      case false =>
    }
  }

  def filterUnselectedTasks(array: js.Array[Double]): js.Array[Double] = {
    scope.tasks.selected.map(selected => {
      val id = selected.substring(1).toInt
      array(id)
    })
  }

  def fetch(): Unit = {
    println("AppProcessorChartsCtrl fetching data")
    val streamingDag = scope.streamingDag.get
    val activeProcessorId = scope.activeProcessorId.toOption.get
    val xLabel = new js.Date(js.Date.now).formatted("HH:mm:ss")
    val receivedMessages = streamingDag.getProcessedMessagesByProcessor(streamingDag.meter, "receiveThroughput", activeProcessorId, aggregated=false).right.get
    val sentMessages = streamingDag.getProcessedMessagesByProcessor(streamingDag.meter, "sendThroughput", activeProcessorId, aggregated=false).right.get
    val processingTimes = streamingDag.getProcessedMessagesByProcessor(streamingDag.histogram, "processTime", activeProcessorId, aggregated=false).right.get
    val receiveLatencies = streamingDag.getProcessedMessagesByProcessor(streamingDag.histogram, "receiveLatency", activeProcessorId, aggregated=false).right.get
    scope.receiveMessageRateChart.data = js.Array(
      js.Dynamic.literal(
        x=xLabel,
        y=filterUnselectedTasks(receivedMessages.rate)
      ).asInstanceOf[SkewData]
    )
    scope.sendMessageRateChart.data = js.Array(
      js.Dynamic.literal(
        x=xLabel,
        y=filterUnselectedTasks(sentMessages.rate)
      ).asInstanceOf[SkewData]
    )
    scope.processingTimeChart.data = js.Array(
      js.Dynamic.literal(
        x=xLabel,
        y=filterUnselectedTasks(processingTimes.rate)
      ).asInstanceOf[SkewData]
    )
    scope.receiveLatencyChart.data = js.Array(
      js.Dynamic.literal(
        x=xLabel,
        y=filterUnselectedTasks(receiveLatencies.rate)
      ).asInstanceOf[SkewData]
    )
  }

  def destroy: Unit = {
    interval.cancel(timeoutPromise)
  }

}
