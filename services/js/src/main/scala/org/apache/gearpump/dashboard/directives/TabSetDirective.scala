package org.apache.gearpump.dashboard.directives

import com.greencatsoft.angularjs._
import org.apache.gearpump.dashboard.controllers.{AppMasterScope, TabSetCtrl}
import org.scalajs.dom._

import scala.scalajs.js.annotation.JSExport

@JSExport
@injectable("TabSetDirective")
class TabSetDirective() extends ElementDirective
with TemplatedDirective with IsolatedScope with UseParentScope {
  override val templateUrl: String ="shared/directives/tabset.html"
  transclude = true
  replace = true
  bindings ++= Seq(
    "switchTo" := ""
  )

  println("TabSetDirective")

  def link(scope: AppMasterScope, elems: Seq[Element], attrs: Attributes, controller: Controller[_]*) = {
  }
}
