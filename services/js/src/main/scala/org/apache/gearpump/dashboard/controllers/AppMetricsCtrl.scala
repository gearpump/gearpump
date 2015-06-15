package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.Scope
import com.greencatsoft.angularjs.{AbstractController, injectable}
import org.apache.gearpump.dashboard.services.{UtilService, RestApiService}

import scala.scalajs.js.annotation.JSExport

trait AppMetricsScope extends Scope {

}

@JSExport
@injectable("AppMetricsCtrl")
class AppMetricsCtrl(scope: AppMetricsScope)
  extends AbstractController[AppMetricsScope](scope) {

  println("AppMetricsCtrl")


}
