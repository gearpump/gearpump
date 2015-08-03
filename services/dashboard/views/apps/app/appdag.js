/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.apps.appmaster')

  .controller('AppDagCtrl', ['$scope', '$filter', '$modal', 'conf', 'dagStyle', function($scope, $filter, $modal, conf, dagStyle) {

    var editDagDialog = $modal({
      templateUrl: "views/apps/app/editdag.html",
      backdrop: 'static',
      show: false,
      scope: $scope
    });

    $scope.modify = function(options) {
      $scope.modifyOptions = options;
      editDagDialog.$promise.then(editDagDialog.show);
    };

    $scope.view = function() {
      $scope.switchToTaskTab($scope.selectedNodeId);
    };

    function showNodeContextMenu(nodeId, point) {
      $scope.selectedNodeId = nodeId.toString(); // cast as string for the time being
      var elem = document.getElementById('viscm');
      angular.element(elem).css({left: point.x + 'px', top: point.y + 'px'});
      angular.element(elem).triggerHandler('click');
    }

    $scope.visgraph = {
      options: dagStyle.newOptions({depth: $scope.streamingDag.hierarchyDepth()}),
      data: dagStyle.newData(),
      events: {
        doubleClick: function (data) {
          if (data.nodes.length === 1) {
            $scope.switchToTaskTab(data.nodes[0]);
          }
        },
        oncontext: function (data) {
          var elem = document.getElementById('viscm');
          if (data.hasOwnProperty('node')) {
            showNodeContextMenu(data.node, data.pointer.DOM);
          }
        }
      }
    };

    function removeDeadElements(visDataSet, aliveElementIds) {
      var ids = visDataSet.getIds().filter(function(id) {
        return !(id in aliveElementIds);
      });
      if (ids.length) {
        // Batch removal is faster
        visDataSet.remove(ids);
      }
    }

    function updateVisGraphNodes(visNodes, data) {
      data.weights[-1] = 0; // weight range from 0 to max weight
      var suggestRadius = _rangeMapper(data.weights, dagStyle.nodeRadiusRange());
      var diff = [];

      angular.forEach(data.processors, function (processor, processorId) {
        var label = '[' + processorId + '] ' +
          (processor.description || $filter('lastPart')(processor.taskClass));
        var weight = data.weights[processorId];
        var hierarchyLevels = data.hierarchyLevels[processorId];
        var visNode = visNodes.get(processorId);
        var newVisRadius = d3.round(suggestRadius(weight), 1);
        var newColor = dagStyle.nodeColor(processor.stalling);
        if (!visNode || visNode.label !== label || visNode.size !== newVisRadius ||
          (visNode.color && visNode.color !== newColor)) {
          diff.push({
            id: processorId, level: hierarchyLevels, // once created, the hierarchy level will not change
            label: label,
            size: newVisRadius,
            color: newColor
          });
        }
      });
      if (diff.length) {
        visNodes.update(diff);
      }
    }

    function updateVisGraphEdges(visEdges, data) {
      data.bandwidths[-1] = 0; // bandwidth range from 0 to max bandwidth
      var suggestWidth = _rangeMapper(data.bandwidths, dagStyle.edgeWidthRange());
      var suggestArrowSize = _rangeMapper(data.bandwidths, dagStyle.edgeArrowSizeRange());
      var suggestOpacity = _rangeMapper(data.bandwidths, dagStyle.edgeOpacityRange());
      var diff = [];

      angular.forEach(data.edges, function (edge, edgeId) {
        var bandwidth = data.bandwidths[edgeId];
        var visEdge = visEdges.get(edgeId);
        var newVisWidth = d3.round(suggestWidth(bandwidth), 1);
        if (!visEdge || visEdge.width !== newVisWidth) {
          diff.push({
            id: edgeId, from: edge.source, to: edge.target,
            width: newVisWidth,
            hoverWidth: 0/*delta*/,
            selectionWidth: 0/*delta*/,
            arrows: {
              to: {scaleFactor: d3.round(suggestArrowSize(bandwidth), 1)}
            },
            color: angular.merge ({
                opacity: d3.round(suggestOpacity(bandwidth), 1)
              }, dagStyle.edgeColorSet(bandwidth > 0)
            )
          });
        }
      });
      if (diff.length) {
        visEdges.update(diff);
      }
    }

    $scope.updateMetricsCounter = function () {
      $scope.receivedMessages = $scope.streamingDag.getReceivedMessages();
      $scope.sentMessages = $scope.streamingDag.getSentMessages();
      $scope.processingTime = $scope.streamingDag.getProcessingTime();
      $scope.receiveLatency = $scope.streamingDag.getReceiveLatency();
    };

    /** Redraw VisGraph on demand */
    function redrawVisGraph(dagData) {
      var visNodes = $scope.visgraph.data.nodes;
      var visEdges = $scope.visgraph.data.edges;
      visNodes.setOptions({queue: true});
      visEdges.setOptions({queue: true});
      try {
        removeDeadElements(visNodes, dagData.processors);
        removeDeadElements(visEdges, dagData.edges);
        updateVisGraphNodes(visNodes, dagData);
        updateVisGraphEdges(visEdges, dagData);
      } finally {
        visNodes.setOptions({queue: false});
        visEdges.setOptions({queue: false});
      }
    }

    $scope.$watchCollection('streamingDag', function(val) {
      $scope.dagData = val.getCurrentDag();
      $scope.updateMetricsCounter();
    });

    $scope.$watchCollection('dagData', function(val) {
      redrawVisGraph(val);
    });

    function _rangeMapper(dict, range) {
      var values = d3.values(dict);
      return d3.scale.linear().domain(d3.extent(values)).range(range);
    }
  }])
;
