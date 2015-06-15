package org.apache.gearpump.dashboard.directives

import com.greencatsoft.angularjs._
import org.scalajs.dom._

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport


@JSExport
@injectable("metricsCard")
class MetricsCardDirective() extends ElementDirective
with TemplatedDirective with IsolatedScope {
  override val templateUrl: String ="shared/directives/metricscard.html"
  println("MetricsCardDirective")

  override def link(scope: ScopeType, elems: Seq[Element], attrs: Attributes, controllers: Either[Controller[_], js.Any]*): Unit = {
    println("MetricsCardDirective.link")
    /*
        attrs.$observe('heading', function (value) {
          scope.heading = value;
        });
        attrs.$observe('explanation', function (value) {
          scope.explanation = value;
        });
        attrs.$observe('value', function (value) {
          scope.value = value;
        });
        attrs.$observe('unit', function (value) {
          scope.unit = value;
        });
     */
  }
}
