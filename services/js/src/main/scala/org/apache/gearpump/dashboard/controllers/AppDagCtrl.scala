/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.{Scope, Interval, Promise, Timeout}
import com.greencatsoft.angularjs.{AbstractController, injectable}
import org.apache.gearpump.dashboard.services.{ConfService, DagStyleService, Flags}

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport
import scala.scalajs.js.undefined

@JSExport
@injectable("AppDagCtrl")
class AppDagCtrl(scope: AppMasterScope, timeout: Timeout, interval: Interval, conf: ConfService, dagStyle: DagStyleService)
  extends AbstractController[AppMasterScope](scope) {
  val d3 = js.Dynamic.global.d3
  var timeoutPromise: Promise = _

  def init(): Unit = {
    val doubleClick = js.Dynamic.literal(
      apply=doubleclick _
    ).asInstanceOf[js.Function1[VisGraph,Unit]]
    val doubleClickEvent = js.Dynamic.literal(
      doubleClick = doubleClick
    ).asInstanceOf[DoubleClickEvent]
    scope.visgraph = js.Dynamic.literal(
      options=dagStyle.newOptions(Flags(depth=scope.streamingDag.get.hierarchyDepth())),
      data=dagStyle.newData(),
      events=doubleClickEvent
    ).asInstanceOf[VisGraph]
    redrawVisGraph()
  }

  @JSExport
  def doubleclick(obj: VisGraph): Unit = {
    val selected = obj.getSelectedNodes()
    selected.length match {
      case 1 =>
        scope.switchToTaskTab(selected(0).toInt)
      case _ =>
    }
  }

  import js.JSConverters._

  def rangeMapper(map: Map[_<:Any, Double], range: js.Array[Double]): js.Function1[Double,Double] = {
    val values = map.values.toSeq.toJSArray
    val extent = d3.extent(values)
    val func = d3.scale.linear().domain(extent).range(range)
    func.asInstanceOf[js.Function1[Double,Double]]
  }

  def updateVisGraphNodes(): Unit = {
    val data = scope.streamingDag.get.getProcessorsData
    data.weights += -1 -> 0
    val suggestRadius = rangeMapper(data.weights, dagStyle.nodeRadiusRange())
    var diffs = js.Array[VisNode]()
    data.processors.foreach(pair => {
      val (processorId, processorDescription) = pair
      val description = Option(processorDescription.description) match {
        case Some(desc) =>
          if(desc.length() > 1) {
            desc
          } else {
            scope.lastPart(processorDescription.taskClass)
          }
        case None =>
          scope.lastPart(processorDescription.taskClass)
      }
      val label = "[" + processorId + "] " + description
      val weight = data.weights(processorId)
      val hierarchyLevels = data.hierarchyLevels(processorId)
      val newVisRadius = d3.round(suggestRadius(weight), 1).asInstanceOf[Double]
      val visNode = scope.visgraph.data.nodes.get(processorId)
      visNode == null match {
        case true =>
          diffs.push(js.Dynamic.literal(id=processorId,label=label,level=hierarchyLevels,size=newVisRadius).asInstanceOf[VisNode])
        case false =>
          val node = visNode.asInstanceOf[VisNode]
          if(node.label != label || node.size != newVisRadius) {
            diffs.push(js.Dynamic.literal(id=processorId,label=label,level=hierarchyLevels,size=newVisRadius).asInstanceOf[VisNode])
          }
      }
    })
    scope.visgraph.data.nodes.update(diffs)
  }

  def updateVisGraphEdges(): Unit = {
    val data = scope.streamingDag.get.getEdgesData
    data.bandwidths += "-1" -> 0.0
    val suggestWidth = rangeMapper(data.bandwidths, dagStyle.edgeWidthRange())
    val suggestArrowSize = rangeMapper(data.bandwidths, dagStyle.edgeArrowSizeRange())
    val suggestOpacity = rangeMapper(data.bandwidths, dagStyle.edgeOpacityRange())
    var diffs = js.Array[VisEdge]()
    data.edges.foreach(pair => {
      val (edgeId, edge) = pair
      val bandwidth = data.bandwidths(edgeId).toInt
      val newVisWidth = d3.round(suggestWidth(bandwidth), 1).asInstanceOf[Double]
      val visEdge = scope.visgraph.data.edges.get(edgeId)
      visEdge == null match {
        case true =>
          diffs.push(
            js.Dynamic.literal(
              id=edgeId,
              from=edge.source,
              to=edge.target,
              width=newVisWidth,
              hoverWidth=0,
              selectionWidth=0,
              arrows=js.Dynamic.literal(
                to=js.Dynamic.literal(
                  scaleFactor=d3.round(suggestArrowSize(bandwidth), 1).asInstanceOf[Double]
                ).asInstanceOf[EdgeScale]
              ).asInstanceOf[EdgeArrows],
              color=js.Dynamic.literal(
                opacity=d3.round(suggestOpacity(bandwidth), 1).asInstanceOf[Double],
                color=dagStyle.edgeColorSet(bandwidth > 0)
              ).asInstanceOf[EdgeColor]
            ).asInstanceOf[VisEdge]
          )
        case false =>
          val ledge = visEdge.asInstanceOf[VisEdge]
          if(ledge.width != newVisWidth) {
            diffs.push(
              js.Dynamic.literal(
                id=edgeId,
                from=edge.source,
                to=edge.target,
                width=newVisWidth,
                hoverWidth=0,
                selectionWidth=0,
                arrows=js.Dynamic.literal(
                  to=js.Dynamic.literal(
                    scaleFactor=d3.round(suggestArrowSize(bandwidth), 1).asInstanceOf[Double]
                  ).asInstanceOf[EdgeScale]
                ).asInstanceOf[EdgeArrows],
                color=js.Dynamic.literal(
                  opacity=d3.round(suggestOpacity(bandwidth), 1).asInstanceOf[Double],
                  color=dagStyle.edgeColorSet(bandwidth > 0)
                ).asInstanceOf[EdgeColor]
              ).asInstanceOf[VisEdge]
            )
          }
      }
    })
    scope.visgraph.data.edges.update(diffs)
  }

  def updateMetricsCounter(): Unit = {
    scope.receivedMessages = scope.streamingDag.get.getReceivedMessages(undefined)
    scope.sentMessages = scope.streamingDag.get.getSentMessages(undefined)
    scope.processingTime = scope.streamingDag.get.getProcessingTime(undefined)
    scope.receiveLatency = scope.streamingDag.get.getReceiveLatency(undefined)
  }

  def redrawVisGraph(): Unit = {
    updateVisGraphNodes()
    updateVisGraphEdges()
    updateMetricsCounter()
    timeoutPromise = timeout(redrawVisGraph _, conf.conf.updateVisDagInterval)
  }

  scope.$watch("streamingDag", init _)
  scope.$on("$destroy", () => {
    timeout.cancel(timeoutPromise)
  })
}

