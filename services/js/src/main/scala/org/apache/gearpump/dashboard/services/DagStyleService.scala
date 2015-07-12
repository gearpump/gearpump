package org.apache.gearpump.dashboard.services

import com.greencatsoft.angularjs.{Factory, injectable}
import org.apache.gearpump.dashboard.controllers.{DagData, DataSet}

import scala.scalajs.js
import scala.scalajs.js.annotation.{JSExport, JSExportAll}

@JSExportAll
case class Flags(depth: Int)

@JSExportAll
case class ColorStyle(border: String, background: String)

trait ColorSet extends js.Object {
  val color: String = js.native
  val hover: String = js.native
  val highlight: String = js.native
}

trait HierarchicalLayout extends js.Object {
  val sortMethod: String = js.native
  val direction: String = js.native
  val levelSeparation: Int = js.native
}

trait Layout extends js.Object {
  val hierarchical: HierarchicalLayout = js.native
}

trait NodeFontStyle extends js.Object {
  val size: Int = js.native
  val face: String = js.native
  val strokeColor: String = js.native
  val strokeWidth: Int = js.native
}

trait NodeStyle extends js.Object {
  val shape: String = js.native
  val font: NodeFontStyle = js.native
}

trait ArrowStyle extends js.Object {
  val to: Boolean = js.native
}

trait EdgeFontStyle extends js.Object {
  val size: Int = js.native
  val face: String = js.native
  val align: String = js.native
}

trait EdgeColorStyle extends js.Object {
  val opacity: Double = js.native
}

trait EdgeStyle extends js.Object {
  val arrows: ArrowStyle = js.native
  val font: EdgeFontStyle = js.native
  val color: EdgeColorStyle = js.native
  val smooth: Boolean = js.native
}

trait ToolTipStyle extends js.Object {
  val fontSize: Int = js.native
  val fontFace: String = js.native
  val fontColor: String = js.native
  val color: ColorStyle = js.native
}

trait Interaction extends js.Object {
  val hover: Boolean = js.native
}

trait DagOptions extends js.Object {
  val autoResize: Boolean = js.native
  val interaction: Interaction = js.native
  val width: String = js.native
  val height: String = js.native
  val layout: Layout = js.native
  val nodes: NodeStyle = js.native
  val edges: EdgeStyle = js.native
}

@JSExport
@injectable("dagStyle")
class DagStyleService() {
  val fontFace:String = "lato,'helvetica neue','segoe ui',arial,helvetica,sans-serif"
  val maxNodeRadius:Int = 16
  val verticalMargin = 30
  val levelDistance = 85

  def newOptions(flags: Flags): DagOptions = {
    js.Dynamic.literal(
      autoResize=true,
      interaction=js.Dynamic.literal(hover=true).asInstanceOf[Interaction],
      width="100%",
      height=(maxNodeRadius * (flags.depth + 1) + levelDistance * flags.depth + verticalMargin * 2).toString + "px",
      layout=js.Dynamic.literal(
        hierarchical=js.Dynamic.literal(sortMethod="directed", direction="UD", levelSeparation=levelDistance).asInstanceOf[HierarchicalLayout]
      ).asInstanceOf[Layout],
      nodes=js.Dynamic.literal(
        shape="dot",
        font=js.Dynamic.literal(size=13, face=fontFace, strokeColor="#fff", strokeWidth=5).asInstanceOf[NodeFontStyle]
      ).asInstanceOf[NodeStyle],
      edges=js.Dynamic.literal(
        arrows=js.Dynamic.literal(to=true).asInstanceOf[ArrowStyle],
        font=js.Dynamic.literal(size=11,face=fontFace,align="middle").asInstanceOf[EdgeFontStyle],
        color=js.Dynamic.literal(opacity=0.75).asInstanceOf[EdgeColorStyle],
        smooth=true).asInstanceOf[EdgeStyle]
    ).asInstanceOf[DagOptions]
  }

  def newData(): DagData = {
    js.Dynamic.literal(
      nodes=new DataSet(),
      edges=new DataSet()
    ).asInstanceOf[DagData]
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

  def edgeColorSet(alive: Boolean): String = {
    alive match {
      case true =>
        "rgb(166,166,166)"
      case false =>
        //js.Dynamic.literal(color="rgb(195,195,195)", hover="rgb(166,166,166)", highlight="rgb(166,166,166)").asInstanceOf[ColorSet]
        "rgb(166,166,166)"
    }
  }
}

@JSExport
@injectable("dagStyle")
class DagStyleServiceFactory extends Factory[DagStyleService] {
  override def apply(): DagStyleService = new DagStyleService()
}
