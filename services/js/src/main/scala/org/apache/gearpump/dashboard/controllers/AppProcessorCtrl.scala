package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.Location
import com.greencatsoft.angularjs.{AbstractController, injectable}
import org.apache.gearpump.dashboard.services.ConfService

import scala.scalajs.js.annotation.JSExport

@JSExport
@injectable("AppProcessorCtrl")
class AppProcessorCtrl(scope: AppMasterScope, location: Location, conf: ConfService)
  extends AbstractController[AppMasterScope](scope) {
}
