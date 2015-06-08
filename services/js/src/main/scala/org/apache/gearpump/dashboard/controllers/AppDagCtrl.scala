package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.{Interval, Scope, Timeout}
import com.greencatsoft.angularjs.{AbstractController, Filter, injectable}
import org.apache.gearpump.shared.Messages.{StreamingAppMasterDataDetail, TimeStamp}

import scala.scalajs.js
import scala.scalajs.js.annotation.{JSExportAll, JSExport}


trait AppDagScope extends Scope {
  var app: StreamingAppMasterDataDetail = js.native
  var displayClock: TimeStamp = js.native
}

@JSExportAll
case class DetectPoint(appClock: TimeStamp, local: TimeStamp)

@JSExport
@injectable("AppDagCtrl")
class AppDagCtrl(scope: AppDagScope, timeout: Timeout, interval: Interval)
  extends AbstractController[AppDagScope](scope) {

  var updateClockPromise: js.Function = _
  var windowSize: Int = 5
  val detectPoint: DetectPoint = DetectPoint(scope.app.clock, System.currentTimeMillis())
  var clockPoints: Seq[DetectPoint] = (0 until windowSize).map(i => {
    detectPoint.copy(local = detectPoint.local - i * 1000)
  }).reverse
  var appClockRate = 0L

  scope.$on("$destroy", updateClockPromise)

  scope.$watch("app.clock", (nowAppClock: TimeStamp) => {
    scope.displayClock = scope.app.clock
    val nowLocal = System.currentTimeMillis()
    val previousDetectPoint = clockPoints.take(1).head
    clockPoints = clockPoints.drop(1)
    val lastClockPoint = clockPoints.last
    lastClockPoint.local * lastClockPoint.appClock > 0 && nowAppClock - lastClockPoint.appClock > 0 match {
      case true =>
        appClockRate = (nowLocal - previousDetectPoint.local) / (nowAppClock  - previousDetectPoint.appClock)
        val localClockInterval = nowLocal - lastClockPoint.local
        timeout(() => {
          scope.displayClock += (localClockInterval/2) / appClockRate
        }, (localClockInterval/2).toInt)
        var nowClock = DetectPoint(nowAppClock, nowLocal)
        clockPoints = clockPoints :+ nowClock
      case false =>
    }

  })

}

