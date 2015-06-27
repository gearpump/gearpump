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

import com.greencatsoft.angularjs.core.{Interval, Promise, Timeout}
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
    scope.visgraph = VisGraph(
      options=dagStyle.newOptions(Flags(depth=scope.streamingDag.hierarchyDepth())),
      data=dagStyle.newData(),
      events=DoubleClickEvent(doubleclick _)
    )
    redrawVisGraph
  }

  def doubleclick(data: DagData): Unit = {
    data.nodes.length match {
      case 1 =>
        scope.switchToTaskTab(/*data.nodes(0)*/0)
      case _ =>
    }
  }

  def rangeMapper(dict: Map[_<:Any, Double], range: js.Array[Double]): js.Function1[Double,Double] = {
    val values = d3.values(dict)
    d3.scale.linear().domain(d3.extent(values)).range(range).asInstanceOf[js.Function1[Double,Double]]
  }

  def updateVisGraphNodes(): Unit = {
    val visNodes = scope.visgraph.data.nodes
    val data = scope.streamingDag.getProcessorsData()
    data.weights += -1 -> 0
    val suggestRadius = rangeMapper(data.weights, dagStyle.nodeRadiusRange())
    var diffs = js.Array[VisNode]()
    data.processors.map(pair => {
      val (processorId, processorDescription) = pair
      val description = Option(processorDescription.description) match {
        case Some(desc) =>
          desc
        case None =>
          scope.lastPart(processorDescription.taskClass)
      }
      val label = "[" + processorId + "] " + description
      val weight = data.weights(processorId)
      val hierarchyLevels = data.hierarchyLevels(processorId)
      val visNode = visNodes.get(processorId)
      val newVisRadius = d3.round(suggestRadius(weight), 1).asInstanceOf[Int]
      visNode.label != label || visNode.size != newVisRadius match {
        case true =>
           diffs.push(VisNode(id=processorId,label=label,level=hierarchyLevels,size=newVisRadius))
        case false =>
      }
    })
    visNodes.update(diffs)
  }

  def updateVisGraphEdges(): Unit = {
    val visEdges = scope.visgraph.data.edges
    val data = scope.streamingDag.getEdgesData()
    data.bandwidths += "-1" -> 0.0
    val suggestWidth = rangeMapper(data.bandwidths, dagStyle.edgeWidthRange())
    val suggestArrowSize = rangeMapper(data.bandwidths, dagStyle.edgeArrowSizeRange())
    val suggestOpacity = rangeMapper(data.bandwidths, dagStyle.edgeOpacityRange())
    var diffs = js.Array[VisEdge]()
    data.edges.foreach(pair => {
      val (edgeId, edge) = pair
      val bandwidth = data.bandwidths(edgeId)
      val visEdge = visEdges.get(edgeId)
      val newVisWidth = d3.round(suggestWidth(bandwidth), 1).asInstanceOf[Int]
      visEdge.width != newVisWidth match {
        case true =>
          diffs.push(
            VisEdge(
              id=edgeId,
              width=newVisWidth,
              hoverWidth=0,
              selectionWidth=0,
              arrows=EdgeArrows(to=EdgeScale(scaleFactor=d3.round(suggestArrowSize(bandwidth), 1).asInstanceOf[Double])),
              color=EdgeColor(opacity=d3.round(suggestOpacity(bandwidth), 1).asInstanceOf[Double],
              color=dagStyle.edgeColorSet(bandwidth > 0))
            )
          )
        case false =>
      }
    })
    visEdges.update(diffs)
  }

  def updateMetricsCounter(): Unit = {
    scope.receivedMessages = scope.streamingDag.getReceivedMessages(undefined)
    scope.sentMessages = scope.streamingDag.getSentMessages(undefined)
    scope.processingTime = scope.streamingDag.getProcessingTime(undefined)
    scope.receiveLatency = scope.streamingDag.getReceiveLatency(undefined)
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

