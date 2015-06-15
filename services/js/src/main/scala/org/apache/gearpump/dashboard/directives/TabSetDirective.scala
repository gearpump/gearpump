package org.apache.gearpump.dashboard.directives

import com.greencatsoft.angularjs._
import org.scalajs.dom._

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport

@JSExport
@injectable("tabset")
class TabSetDirective() extends ElementDirective with TemplatedDirective with IsolatedScope with UseParentScope {
  override val templateUrl: String ="shared/directives/tabset.html"
  transclude = true
  replace = true
  bindings ++: Seq(
    "switchTo" := ""
  )

  println("TabSetDirective")

  override def link(scope: ScopeType, elems: Seq[Element], attrs: Attributes, controllers: Either[Controller[_],js.Any]*):Unit = {
    println("TabSetDirective.link")
  }
}
