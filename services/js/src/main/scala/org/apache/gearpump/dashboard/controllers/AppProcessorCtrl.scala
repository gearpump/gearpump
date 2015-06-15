package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.{Location, Scope}
import com.greencatsoft.angularjs.{AbstractController, injectable}
import org.apache.gearpump.dashboard.filters.LastPartFilter
import org.apache.gearpump.dashboard.services.ConfService

import scala.scalajs.js.annotation.JSExport

trait AppProcessorScope extends Scope {

}

@JSExport
@injectable("AppProcessorCtrl")
class AppProcessorCtrl(scope: AppProcessorScope, location: Location, conf: ConfService, lastPart: LastPartFilter)
  extends AbstractController[AppProcessorScope](scope) {

  println("AppProcessorCtrl")


}
