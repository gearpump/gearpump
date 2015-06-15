package org.apache.gearpump.dashboard.directives

import com.greencatsoft.angularjs.{Attributes, Controller, ElementDirective, TemplatedDirective, injectable}
import org.scalajs.dom.Element

import scala.scalajs.js.annotation.JSExport

@JSExport
@injectable("ExplanationIconDirective")
class ExplanationIconDirective extends ElementDirective with TemplatedDirective {

  override val templateUrl = "<span class=\"glyphicon glyphicon-question-sign metrics-explanation\" bs-tooltip=\"value\"></span>"

  def link(scope: ScopeType, elems: Seq[Element], attrs: Attributes, controller: Controller[_]*) = {
    scope.$apply(attrs)
  }
}
