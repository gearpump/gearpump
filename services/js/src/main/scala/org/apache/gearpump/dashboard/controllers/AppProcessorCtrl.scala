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

  def available(): js.Array[String] = {
    (0 until scope.processor.parallelism).map("T"+_).toJSArray
  }

  def formatter(params: js.Array[SkewParams]): String = {
    s"<strong>${params.head.name}</strong><br/>${params.head.data} msgs/s"
  }

  def init(): Unit = {
    val streamingDag = scope.streamingDag.get
    val activeProcessorId = scope.activeProcessorId.toOption.foreach(activeProcessorId => {
      scope.processor = streamingDag.processors(activeProcessorId)
      scope.processorConnections = streamingDag.calculateProcessorConnections(activeProcessorId)
      val skewData = streamingDag.getReceivedMessages(activeProcessorId).rate
      scope.tasks = js.Dynamic.literal(
        available = available(),
        selected = js.Array[String]()
      ).asInstanceOf[Tasks]
      val dataZoom = js.Dynamic.literal(
        show=false
      ).asInstanceOf[SkewDataZoom]
      val skewSeriesData = js.Dynamic.literal(
        name="s1",
        data=skewData,
        `type`="bar",
        clickable=true
      ).asInstanceOf[SkewSeriesData]
      val inject=js.Dynamic.literal(
        height="110px",
        xAxisDataNum=scope.processor.parallelism,
        dataZoom=dataZoom
      ).asInstanceOf[SkewInject]
      val options = js.Dynamic.literal(
        inject=inject,
        series=js.Array[SkewSeriesData](skewSeriesData),
        xAxisData=scope.tasks.available
      ).asInstanceOf[SkewOptions]
      scope.receiveSkewChart = js.Dynamic.literal(
        options=options,
        data=js.Array[SkewData]()
      ).asInstanceOf[SkewChart]
      timeoutPromise = interval(fetch _, conf.conf.updateChartInterval)
    })
  }
  /*
  good
  "{
  "tooltip": {
    "trigger": "item",
    "textStyle": {
      "fontSize": 12
    },
    "axisPointer": {
      "type": "line",
      "lineStyle": {
        "color": "rgba(0,119,215,.2)",
        "width": 2,
        "type": "dotted"
      }
    },
    "borderRadius": 2
  },
  "dataZoom": {
    "show": false
  },
  "grid": {
    "borderWidth": 0,
    "x": 5,
    "y": 5,
    "x2": 5,
    "y2": 30
  },
  "xAxis": [
    {
      "axisLabel": {
        "show": true
      },
      "axisLine": {
        "show": false
      },
      "axisTick": {
        "show": false
      },
      "splitLine": {
        "show": false
      },
      "data": [
        "T0"
      ]
    }
  ],
  "yAxis": [
    {
      "show": false
    }
  ],
  "series": [
    {
      "colors": {
        "line": "rgb(0,119,215)",
        "grid": "rgba(0,119,215,.2)",
        "area": "rgb(229,242,250)"
      },
      "type": "bar",
      "name": "s1",
      "data": [
        389688.0187495387
      ],
      "clickable": true,
      "smooth": true,
      "itemStyle": {
        "normal": {
          "areaStyle": {
            "type": "default",
            "color": "rgb(229,242,250)"
          },
          "lineStyle": {
            "color": "rgb(0,119,215)",
            "width": 3
          }
        },
        "emphasis": {
          "color": "rgb(0,119,215)"
        }
      }
    }
  ],
  "height": "110px",
  "xAxisDataNum": 1
}"
   */

  /*
  bad
  "{
  "tooltip": {
    "trigger": "item",
    "textStyle": {
      "fontSize": 12
    },
    "axisPointer": {
      "type": "line",
      "lineStyle": {
        "color": "rgba(0,119,215,.2)",
        "width": 2,
        "type": "dotted"
      }
    },
    "borderRadius": 2,
    "formatter": {}
  },
  "dataZoom": {
    "show": true,
    "realtime": true,
    "y": 0,
    "height": 20
  },
  "grid": {
    "borderWidth": 0,
    "x": 5,
    "y": 5,
    "x2": 5,
    "y2": 30
  },
  "xAxis": [
    {
      "axisLabel": {
        "show": true
      },
      "axisLine": {
        "show": false
      },
      "axisTick": {
        "show": false
      },
      "splitLine": {
        "show": false
      },
      "data": [
        "T0"
      ]
    }
  ],
  "yAxis": [
    {
      "show": false
    }
  ],
  "series": [
    {
      "colors": {
        "line": "rgb(0,119,215)",
        "grid": "rgba(0,119,215,.2)",
        "area": "rgb(229,242,250)"
      },
      "type": "bar",
      "name": "s1",
      "data": 29.283640652292455,
      "clickable": true,
      "smooth": true,
      "itemStyle": {
        "normal": {
          "areaStyle": {
            "type": "default",
            "color": "rgb(229,242,250)"
          },
          "lineStyle": {
            "color": "rgb(0,119,215)",
            "width": 3
          }
        },
        "emphasis": {
          "color": "rgb(0,119,215)"
        }
      }
    }
  ],
  "height": "110px",
  "xAxisDataNum": 1
}"
   */

  def fetch(): Unit = {
    val streamingDag = scope.streamingDag.get
    val receivedMessages = streamingDag.getProcessedMessagesByProcessor(
      streamingDag.meter,
      "receiveThroughput",
      scope.activeProcessorId.get,
      aggregated=false
    ).right.get
    val skewData = receivedMessages.rate
    val data = (0 until scope.processor.parallelism).map(i => {
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
