package org.apache.gearpump.dashboard.services

import com.greencatsoft.angularjs.{injectable, Factory}

import scala.scalajs.js
import scala.scalajs.js.annotation.{JSExport, JSName}

case class Flags(depth: Int)
case class ColorStyle(border: String, background: String)
case class HierarchicalLayout(layout: String, direction: String, levelSeparation: Int)
case class NodeStyle(shape: String, fontSize: Int, fontFace: String, fontStrokeColor: String, fontStrokeWidth: Int)
case class EdgeStyle(style: String, labelAlignment: String, fontSize: Int, fontFace: String, widthSelectionMultiplier: Int, opacity: Float)
case class ToolTipStyle(fontSize: Int, fontFace: String, fontColor: String, color: ColorStyle)
case class DagOptions(hover: Boolean, width: String, height: String, hierarchicalLayout: HierarchicalLayout,
                 stabilize: Boolean, freezeForStablization: Boolean, nodes: NodeStyle, edges: EdgeStyle, tooltip: ToolTipStyle)

@JSName("vis.DataSet")
class DataSet extends js.Object {
  val length: Int = js.native
  def add(data: Array[js.Object], senderId: String): Array[String] = js.native
}

case class DagData(nodes: DataSet, edges: DataSet)

@injectable("DagStyleService")
class DagStyleService() {
  import DagStyleServiceFactory._
  val verticalMargin = 30
  val levelDistance = 85
  def newOptions(flags: Flags): DagOptions = {
    DagOptions(true, "100%", (maxNodeRadius * (flags.depth + 1) + levelDistance * flags.depth + verticalMargin * 2).toString + "px",
    HierarchicalLayout("direction", "UD", levelDistance), true, true, NodeStyle("dot", 13, fontFace, "#fff", 5),
    EdgeStyle("arrow", "line-center", 11, fontFace, 1, 0.75F), ToolTipStyle(12, fontFace, "#000", ColorStyle("#eee", "#fff")))
  }
  def newData: DagData = {
    DagData(new DataSet(), new DataSet())
  }

}

@JSExport
@injectable("DagStyleService")
object DagStyleServiceFactory extends Factory[DagStyleService] {
  val fontFace:String = "lato,'helvetica neue','segoe ui',arial,helvetica,sans-serif"
  val maxNodeRadius:Int = 16
  override def apply(): DagStyleService = new DagStyleService()
}
