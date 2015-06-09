package org.apache.gearpump.dashboard.directives

import com.greencatsoft.angularjs._
import com.greencatsoft.angularjs.core.Compile
import org.apache.gearpump.dashboard.controllers.{Tab, AppMasterScope, TabSetCtrl}
import org.apache.gearpump.dashboard.services.RestApiService
import org.scalajs.dom._

import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js.annotation.JSExport
import scala.util.{Failure, Success}

@JSExport
@injectable("TabDirective")
class TabDirective(restApi: RestApiService, ctrl: TabSetCtrl, compile: Compile) extends ElementDirective
with TemplatedDirective with IsolatedScope with UseParentScope with Requires {
  override val templateUrl: String ="shared/directives/tab.html"
  transclude = true
  replace = true
  bindings ++= Seq(
    "switchTo" := ""
  )
  requirements ++= Seq(new Requirement("tabset", true))

  println("TabDirective")

  def link(scope: AppMasterScope, elems: Seq[Element], attrs: Attributes, controllers: Controller[_]*) = {
    println("TabDirective.link")
    val elem = elems.head
    attrs.$get("heading").toOption.foreach(heading => {
      scope.selectedTab.heading = heading
      attrs.$get("template").toOption.foreach(template => {
        scope.load = (reload:Boolean) => {
          restApi.subscribe(template) onComplete {
            case Success(data) =>
              val templateScope = scope.$new(true).asInstanceOf[AppMasterScope]
              elem.innerHTML = template
              attrs.$get("controller").toOption.map(controller => {
                (0 to elem.children.length).foreach(i => {
                  ctrl(templateScope)
                  elem.children(i).setAttribute("$ngController", "TabSetCtrl")
                })
                compile(elem, null, 0)(templateScope, null)
              })
            case Failure(t) =>
              println(s"Failed to get workers ${t.getMessage}")
          }
        }
      })
    })
    ctrl.addTab(scope.selectedTab)
  }
}
