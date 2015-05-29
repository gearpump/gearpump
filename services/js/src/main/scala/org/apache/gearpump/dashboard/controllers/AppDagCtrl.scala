package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.{Timeout, Interval, Scope}
import com.greencatsoft.angularjs.{Filter, AbstractController, injectable}
import org.apache.gearpump.cluster.Messages.{TimeStamp, StreamingAppMasterDataDetail}

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport


trait AppDagScope extends Scope {
  var app: StreamingAppMasterDataDetail
  var displayClock: TimeStamp
}
case class DetectPoint(appClock: TimeStamp, local: TimeStamp)

@JSExport
@injectable("appDagCtrl")
class AppDagCtrl(scope: AppDagScope, timeout: Timeout, interval: Interval, filter: Filter[String])
  extends AbstractController[AppDagScope](scope) {

  var updateClockPromise: js.Function = _
  var windowSize: Int = 5
  val detectPoint: DetectPoint = DetectPoint(scope.app.clock, System.currentTimeMillis())
  val clockPoints: Seq[DetectPoint] = (0 until windowSize).map(i => {
    detectPoint.copy(local = detectPoint.local - i * 1000)
  }).reverse

  scope.$on("$destroy", updateClockPromise)

  scope.$watch("app.clock", (nowAppClock: TimeStamp) => {
    scope.displayClock = scope.app.clock
  })

}

