package org.apache.gearpump.dashboard.filters

import com.greencatsoft.angularjs.{Filter, injectable}

import scala.scalajs.js
import scala.scalajs.js.UndefOr
import scala.scalajs.js.UndefOr._
import scala.scalajs.js.annotation.JSExport

@JSExport
@injectable("humanize")
class HumanizeFilter extends Filter[UndefOr[String]] {
  override def filter(value: UndefOr[String]): UndefOr[String] = {
    value.isDefined match {
      case true =>
        val num = value.toString
        val number = num.toInt
        if (number < 1000) {
          number.toString
        }
        val si = List("K", "M", "G", "T", "P", "H")
        val exp = Math.floor(Math.log(number) / Math.log(1000))
        val result = number / Math.pow(1000, exp)
        (result % 1 > (1 / Math.pow(1000, exp - 1))) match {
          case true =>
            f"$result%1.2f${si((exp-1).toInt)}"
          case false =>
            f"$result%1.0f${si((exp-1).toInt)}"
        }
      case false =>
        ""
    }
  }
}
