package org.apache.gearpump.dashboard.services

import com.greencatsoft.angularjs.{Factory, injectable}
import org.apache.gearpump.dashboard.controllers.{DataSet, DagData}

import scala.scalajs.js
import scala.scalajs.js.annotation.{JSExport, JSExportAll}

@JSExportAll
case class Flags(depth: Int)

@JSExportAll
case class ColorStyle(border: String, background: String)

@JSExportAll
case class ColorSet(color: String, hover: String, highlight: String)

@JSExportAll
case class HierarchicalLayout(sortMethod: String, direction: String, levelSeparation: Int)

@JSExportAll
case class Layout(hierarchical: HierarchicalLayout)

@JSExportAll
case class NodeFontStyle(size: Int, face: String, strokeColor: String, strokeWidth: Int)

@JSExportAll
case class NodeStyle(shape: String, font: NodeFontStyle)

@JSExportAll
case class ArrowStyle(to: Boolean)

@JSExportAll
case class EdgeFontStyle(size: Int, face: String, align: String)

@JSExportAll
case class EdgeColorStyle(opacity: Double)

@JSExportAll
case class EdgeStyle(arrows: ArrowStyle, font: EdgeFontStyle, color: EdgeColorStyle, smooth: Boolean)

@JSExportAll
case class ToolTipStyle(fontSize: Int, fontFace: String, fontColor: String, color: ColorStyle)

@JSExportAll
case class Interaction(hover: Boolean)

@JSExportAll
case class DagOptions(autoResize: Boolean, interaction: Interaction, width: String, height: String, layout: Layout, nodes: NodeStyle, edges: EdgeStyle)

@JSExport
@injectable("dagStyle")
class DagStyleService() {
  val fontFace:String = "lato,'helvetica neue','segoe ui',arial,helvetica,sans-serif"
  val maxNodeRadius:Int = 16
  val verticalMargin = 30
  val levelDistance = 85

  def newOptions(flags: Flags): DagOptions = {
    DagOptions(
      autoResize=true,
      interaction=Interaction(hover=true),
      width="100%",
      height=(maxNodeRadius * (flags.depth + 1) + levelDistance * flags.depth + verticalMargin * 2).toString + "px",
      layout=Layout(hierarchical=HierarchicalLayout(sortMethod="direction", direction="UD", levelSeparation=levelDistance)),
      nodes=NodeStyle(shape="dot", font=NodeFontStyle(size=13, face=fontFace, strokeColor="#fff", strokeWidth=5)),
      edges=EdgeStyle(arrows=ArrowStyle(to=true), font=EdgeFontStyle(size=11,face=fontFace,align="middle"),color=EdgeColorStyle(opacity=0.75),smooth=true)
    )
  }

  def newData(): DagData = {
    DagData(new DataSet(), new DataSet())
  }

  def nodeRadiusRange(): js.Array[Double] = {
    js.Array(2, 16)
  }

  def edgeWidthRange(): js.Array[Double] = {
    js.Array(1, 5)
  }

  def edgeArrowSizeRange(): js.Array[Double] = {
    js.Array(0.5, 0.1)
  }

  def edgeOpacityRange(): js.Array[Double] = {
    js.Array(0.4, 1.0)
  }

  def edgeColorSet(alive: Boolean): ColorSet = {
    alive match {
      case true =>
        ColorSet(color="black", hover="black", highlight="black")
      case false =>
        ColorSet(color="rgb(195,195,195)", hover="rgb(166,166,166)", highlight="rgb(166,166,166)")
    }
  }
}

@JSExport
@injectable("dagStyle")
class DagStyleServiceFactory extends Factory[DagStyleService] {
  override def apply(): DagStyleService = new DagStyleService()
}
