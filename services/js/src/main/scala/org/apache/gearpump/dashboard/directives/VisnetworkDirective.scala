package org.apache.gearpump.dashboard.directives

import com.greencatsoft.angularjs.core.Scope
import com.greencatsoft.angularjs._
import org.scalajs.dom.Element

import scala.scalajs.js
import scala.scalajs.js.UndefOr
import scala.scalajs.js.annotation.JSExport

trait VisnetworkScope extends Scope {
  var data: js.Array[String] = js.native
  var options: js.Array[String] = js.native
  var events: js.Array[String] = js.native
}

@JSExport
@injectable("visnetworkDirective")
class VisnetworkDirective extends ElementDirective with AttributeDirective with IsolatedScope {

  println("VisnetworkDirective")

  bindings ++= Seq(
    "data" := "",
    "options" := "",
    "events" := ""
  )

  def link(scope: VisnetworkScope, elems: Seq[Element], attrs: Attributes, controller: Controller[_]*) = {
    val network = js.Dynamic.newInstance(js.Dynamic.global.vis.Network)(elems.head, scope.data, scope.options)

    scope.$watch(attrs("data"), (data: UndefOr[js.Any]) => {
      if (data.isDefined) {
        network.setData(data)
        network.freezeSimulation(true)
      }
    })
    scope.$watchCollection(attrs("options"), (options: UndefOr[js.Any]) => {
      if (options.isDefined) {
        network.setOptions(options)
      }
    })
  }
}
