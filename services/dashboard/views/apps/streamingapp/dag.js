/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .config(['$stateProvider',
    function($stateProvider) {
      'use strict';

      $stateProvider
        .state('streamingapp.dag', {
          url: '/dag',
          templateUrl: 'views/apps/streamingapp/dag.html',
          controller: 'StreamingAppDagCtrl'
        });
    }])

  .controller('StreamingAppDagCtrl', ['$scope', '$state', '$modal', '$contextmenu', '$visNetworkStyle',
    function($scope, $state, $modal, $contextmenu, $vis) {
      'use strict';

      // todo: while metrics is not available, keep loading, otherwise the bandwidth is not calculable.

      var editorDialog = $modal({
        templateUrl: "views/apps/streamingapp/popups/dag_edit.html",
        backdrop: 'static',
        show: false,
        scope: $scope,
        controller: 'StreamingAppDagEditCtrl'
      });

      $scope.modify = function(options) {
        $scope.modifyOptions = options;
        editorDialog.$promise.then(editorDialog.show);
      };

      $scope.visGraph = {
        options: $vis.newHierarchicalLayoutOptions({depth: $scope.dag.hierarchyDepth()}),
        data: $vis.newData(),
        events: {
          doubleClick: function(data) {
            if (data.nodes.length === 1) {
              var nodeId = parseInt(data.nodes[0]);
              $state.go('streamingapp.processor', {processorId: nodeId});
            }
          },
          oncontext: function(data) {
            if (data.hasOwnProperty('node')) {
              $scope.activeProcessor = $scope.dag.processors[data.node];
              showNodeOperationsContextMenu(data.node, data.pointer.DOM);
            }
          }
        }
      };

      function showNodeOperationsContextMenu(nodeId, position) {
        var elem = document.getElementById('dag-node-menu');
        $scope.view = function() {
          $state.go('streamingapp.processor', {processorId: nodeId});
        };
        $contextmenu.popup(elem, position);
      }

      /** Scope independent draw methods */
      var draw = {
        removeDeadElements: function(visDataSet, aliveElementIds) {
          var ids = visDataSet.getIds().filter(function(id) {
            return !aliveElementIds.hasOwnProperty(id);
          });
          if (ids.length) {
            // Batch removal is faster
            visDataSet.remove(ids);
          }
        },

        updateVisGraphNodes: function(visNodes, data) {
          data.weights[-1] = 0; // weight range from 0 to max weight
          var suggestRadiusFn = draw._rangeMapper(data.weights, $vis.nodeRadiusRange());
          var diff = [];

          _.forEach(data.processors, function(processor, processorId) {
            var label = $vis.processorNameAsLabel(processor);
            var weight = data.weights[processorId];
            var hierarchyLevel = data.hierarchyLevels[processorId];
            var visNode = visNodes.get(processorId);
            var size = d3.round(suggestRadiusFn(weight), 1);
            var color = $vis.nodeColor(processor.isStalled);
            if (!visNode || visNode.label !== label || visNode.size !== size ||
              (visNode.color && visNode.color !== color)) {
              diff.push({
                id: processorId, level: hierarchyLevel, // once created, the hierarchy level will not change
                label: label,
                size: size,
                color: color
              });
            }
          });
          if (diff.length) {
            visNodes.update(diff);
          }
        },

        updateVisGraphEdges: function(visEdges, data) {
          data.bandwidths[-1] = 0; // minimal bandwidth
          var suggestEdgeWidthFn = draw._rangeMapper(data.bandwidths, $vis.edgeWidthRange());
          var suggestEdgeArrowSizeFn = draw._rangeMapper(data.bandwidths, $vis.edgeArrowSizeRange());
          var suggestEdgeOpacityFn = draw._rangeMapper(data.bandwidths, $vis.edgeOpacityRange());
          var diff = [];

          _.forEach(data.edges, function(edge, edgeId) {
            var bandwidth = data.bandwidths[edgeId];
            var visEdge = visEdges.get(edgeId);
            var width = d3.round(suggestEdgeWidthFn(bandwidth), 1);
            if (!visEdge || visEdge.width !== width) {
              diff.push({
                id: edgeId, from: edge.source, to: edge.target,
                width: width,
                hoverWidth: 0 /*delta*/,
                selectionWidth: 0 /*delta*/,
                arrows: {
                  to: {scaleFactor: d3.round(suggestEdgeArrowSizeFn(bandwidth), 1)}
                },
                color: angular.merge({
                    opacity: d3.round(suggestEdgeOpacityFn(bandwidth), 1)
                  }, $vis.edgeColorSet(bandwidth > 0)
                )
              });
            }
          });
          if (diff.length) {
            visEdges.update(diff);
          }
        },

        _rangeMapper: function(dict, range) {
          var values = d3.values(dict);
          return d3.scale.linear().domain(d3.extent(values)).range(range);
        }
      };

      function redrawGraph(dagData) {
        var visNodes = $scope.visGraph.data.nodes;
        var visEdges = $scope.visGraph.data.edges;
        visNodes.setOptions({queue: true});
        visEdges.setOptions({queue: true});
        try {
          draw.removeDeadElements(visNodes, dagData.processors);
          draw.removeDeadElements(visEdges, dagData.edges);
          draw.updateVisGraphNodes(visNodes, dagData);
          draw.updateVisGraphEdges(visEdges, dagData);
        } finally {
          visNodes.setOptions({queue: false});
          visEdges.setOptions({queue: false});
        }
      }

      function updateMetricsValues(metricsProvider) {
        var receivedMessages = metricsProvider.getReceivedMessages();
        var sentMessages = metricsProvider.getSentMessages();
        $scope.currentMessageSendRate = sentMessages.rate;
        $scope.currentMessageReceiveRate = receivedMessages.rate;
        $scope.totalSentMessages = sentMessages.total;
        $scope.totalReceivedMessages = receivedMessages.total;
        $scope.averageProcessingTime = metricsProvider.getMessageProcessingTime();
        $scope.averageTaskReceiveLatency = metricsProvider.getMessageReceiveLatency();
      }

      $scope.$watchCollection('dag', function(dag) {
        var dagData = dag.getCurrentDag();
        redrawGraph(dagData);
        updateMetricsValues(dag);
      });
    }])
;