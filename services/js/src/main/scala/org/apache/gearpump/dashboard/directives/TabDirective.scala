package org.apache.gearpump.dashboard.directives

import com.greencatsoft.angularjs._
import org.apache.gearpump.dashboard.controllers.{AppMasterScope, Tab}
import org.scalajs.dom._
import org.scalajs.dom.raw.HTMLElement

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport

@JSExport
@injectable("tab")
class TabDirective() extends ElementDirective
with TemplatedDirective with UseParentScope with Requires {
  override val templateUrl: String ="shared/directives/tab.html"
  transclude = true
  replace = true
  requirements ++: Option(^("tabset"))

  println("TabDirective")

  override def link(scope: ScopeType, elems: Seq[Element], attrs: Attributes, controllers: Either[Controller[_], js.Any]*): Unit = {
    println("TabDirective.link")
    val elem = elems.head.asInstanceOf[HTMLElement]
    val appMasterScope = scope.dynamic.$parent.$parent.$parent.asInstanceOf[AppMasterScope]
    val tab = scope.dynamic.tab.asInstanceOf[Tab]
    scope.$watch(attrs("switchTo"), () => {
      appMasterScope.selectTab(tab)
      appMasterScope.load(elem, tab)
    })

    //tabSetCtrl.addTab(appMasterScope.selectedTab)
  }
}
