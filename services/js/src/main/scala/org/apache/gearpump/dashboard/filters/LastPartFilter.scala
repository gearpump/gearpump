package org.apache.gearpump.dashboard.filters

import com.greencatsoft.angularjs.{Filter, injectable}

import scala.scalajs.js.annotation.JSExport

@JSExport
@injectable("LastPartFilter")
class LastPartFilter extends Filter[String] {
  override def filter(name: String): String = {
    name.split("\\.").last
  }
}
