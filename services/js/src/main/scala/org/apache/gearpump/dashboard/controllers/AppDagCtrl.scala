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

import com.greencatsoft.angularjs.core.{Interval, Timeout}
import com.greencatsoft.angularjs.{AbstractController, injectable}
import org.apache.gearpump.dashboard.filters.LastPartFilter
import org.apache.gearpump.dashboard.services.{DagStyleService, Flags}

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport

@JSExport
@injectable("AppDagCtrl")
class AppDagCtrl(scope: AppMasterScope, timeout: Timeout, interval: Interval, dagStyle: DagStyleService, lastPart: LastPartFilter)
  extends AbstractController[AppMasterScope](scope) {
  val d3 = js.Dynamic.global.d3

  def doubleclick(data: DagData): Unit = {
    data.nodes.length match {
      case 1 =>
        scope.switchToTaskTab(/*data.nodes(0)*/0)
      case _ =>
    }
  }

  def rangeMapper(dict: Map[Int, Double], range: js.Array[Int]): js.Function1[Double,Double] = {
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
          lastPart.filter(processorDescription.taskClass)
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

  }

  def updateMetricsCounter(): Unit = {

  }

  scope.visgraph = VisGraph(
    options=dagStyle.newOptions(Flags(depth=scope.streamingDag.hierarchyDepth())),
    data=dagStyle.newData(),
    events=DoubleClickEvent(doubleclick _)
  )

 /*

    $scope.updateVisGraphEdges = function () {
      var visEdges = $scope.visgraph.data.edges;
      var data = $scope.streamingDag.getEdgesData();
      data.bandwidths[-1] = 0; // bandwidth range from 0 to max bandwidth
      var suggestWidth = _rangeMapper(data.bandwidths, dagStyle.edgeWidthRange());
      var suggestArrowSize = _rangeMapper(data.bandwidths, dagStyle.edgeArrowSizeRange());
      var suggestOpacity = _rangeMapper(data.bandwidths, dagStyle.edgeOpacityRange());
      var diff = [];

      angular.forEach(data.edges, function (edge, edgeId) {
        var bandwidth = parseInt(data.bandwidths[edgeId]);
        var visEdge = visEdges.get(edgeId);
        var newVisWidth = d3.round(suggestWidth(bandwidth), 1);
        if (!visEdge || visEdge.width !== newVisWidth) {
          diff.push({
            id: edgeId, from: edge.source, to: edge.target,
            width: newVisWidth,
            hoverWidth: 0/*delta*/,
            selectionWidth: 0/*delta*/,
            arrows: {
              to: {
                scaleFactor: d3.round(suggestArrowSize(bandwidth), 1)
              }
            },
            color: {
              opacity: d3.round(suggestOpacity(bandwidth), 1),
              color: dagStyle.edgeColorSet(bandwidth > 0)
            }
          });
        }
      });
      visEdges.update(diff);
    };

    $scope.updateMetricsCounter = function () {
      $scope.receivedMessages = $scope.streamingDag.getReceivedMessages();
      $scope.sentMessages = $scope.streamingDag.getSentMessages();
      $scope.processingTime = $scope.streamingDag.getProcessingTime();
      $scope.receiveLatency = $scope.streamingDag.getReceiveLatency();
    };

    var timeoutPromise;
    $scope.$on('$destroy', function () {
      $timeout.cancel(timeoutPromise);
    });

    /** Redraw VisGraph on demand */
    var redrawVisGraph = function () {
      $scope.updateVisGraphNodes();
      $scope.updateVisGraphEdges();
      $scope.updateMetricsCounter();
      timeoutPromise = $timeout(redrawVisGraph, conf.updateVisDagInterval);
    };
    redrawVisGraph();

    function _rangeMapper(dict, range) {
      var values = d3.values(dict);
      return d3.scale.linear().domain(d3.extent(values)).range(range);
    }
  */
}

