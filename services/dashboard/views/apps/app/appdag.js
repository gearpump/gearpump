/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.apps.appmaster')

  .controller('AppDagCtrl', ['$scope', '$timeout', '$filter', 'conf', 'dagStyle', function ($scope, $timeout, $filter, conf, dagStyle) {
    $scope.visgraph = {
      options: dagStyle.newOptions({depth: $scope.streamingDag.hierarchyDepth()}),
      data: dagStyle.newData(),
      events: {
        doubleClick: function (data) {
          if (data.nodes.length === 1) {
            $scope.switchToTaskTab(data.nodes[0]);
          }
        }
      }
    };

    $scope.updateVisGraphNodes = function () {
      var visNodes = $scope.visgraph.data.nodes;
      var data = $scope.streamingDag.getProcessorsData();
      data.weights[-1] = 0; // weight range from 0 to max weight
      var suggestRadius = _rangeMapper(data.weights, dagStyle.nodeRadiusRange());
      var diff = [];

      angular.forEach(data.processors, function (processor, key) {
        var processorId = parseInt(key);

        var label = '[' + processorId + '] ';
        label += processor.description ? processor.description : $filter('lastPart')(processor.taskClass);

        var weight = parseInt(data.weights[processorId]);
        var hierarchyLevels = data.hierarchyLevels[processorId];
        var visNode = visNodes.get(processorId);
        var newVisRadius = d3.round(suggestRadius(weight), 1);
        if (!visNode || visNode.label !== label || visNode.radius !== newVisRadius) {
          diff.push({
            id: processorId,
            label: label,
            level: hierarchyLevels,
            radius: newVisRadius
          });
        }
      });
      visNodes.update(diff);
    };

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
            hoverWidth: newVisWidth,
            arrowScaleFactor: d3.round(suggestArrowSize(bandwidth), 1),
            opacity: d3.round(suggestOpacity(bandwidth), 1),
            color: dagStyle.edgeColorSet(bandwidth > 0)
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
  }])
;
