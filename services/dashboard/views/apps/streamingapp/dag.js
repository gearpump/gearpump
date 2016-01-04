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
              var processorId = Number(data.nodes[0]);
              $state.go('streamingapp.processor', {processorId: processorId});
            }
          },
          oncontext: function(data) {
            if (data.hasOwnProperty('node')) {
              var processorId = Number(data.node);
              $scope.activeProcessor = $scope.dag.getProcessor(processorId);
              showProcessorOperationsContextMenu(processorId, data.pointer.DOM);
            }
          }
        }
      };

      function showProcessorOperationsContextMenu(processorId, position) {
        var elem = document.getElementById('dag-node-menu');
        $scope.view = function() {
          $state.go('streamingapp.processor', {processorId: processorId});
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
          data.processorWeights[-1] = 0; // weight range from 0 to max weight
          var suggestRadiusFn = draw._rangeMapper(data.processorWeights, $vis.nodeRadiusRange());
          var diff = [];

          _.forEach(data.processors, function(processor) {
            var label = $vis.processorNameAsLabel(processor);
            var weight = data.processorWeights[processor.id];
            var visNode = visNodes.get(processor.id);
            var size = d3.round(suggestRadiusFn(weight), 1);
            var concern = data.processorStallingTasks.hasOwnProperty(processor.id);
            var color = $vis.nodeColor(concern);
            if (!visNode || visNode.label !== label || visNode.size !== size ||
              (visNode.color && visNode.color !== color)) {
              diff.push({
                id: processor.id, level: processor.hierarchy, // once created, the hierarchy will not change
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
          data.edgeBandwidths[-1] = 0; // minimal bandwidth
          var suggestEdgeWidthFn = draw._rangeMapper(data.edgeBandwidths, $vis.edgeWidthRange());
          var suggestEdgeArrowSizeFn = draw._rangeMapper(data.edgeBandwidths, $vis.edgeArrowSizeRange());
          var suggestEdgeOpacityFn = draw._rangeMapper(data.edgeBandwidths, $vis.edgeOpacityRange());
          var diff = [];

          _.forEach(data.edges, function(edge, edgeId) {
            var bandwidth = data.edgeBandwidths[edgeId];
            var visEdge = visEdges.get(edgeId);
            var width = d3.round(suggestEdgeWidthFn(bandwidth), 1);
            if (!visEdge || visEdge.width !== width) {
              diff.push({
                id: edgeId, from: edge.from, to: edge.to,
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
        var receivedMessages = metricsProvider.getSinkProcessorReceivedMessageTotalAndRate();
        var sentMessages = metricsProvider.getSourceProcessorSentMessageTotalAndRate();
        $scope.currentMessageSendRate = sentMessages.rate;
        $scope.currentMessageReceiveRate = receivedMessages.rate;
        $scope.totalSentMessages = sentMessages.total;
        $scope.totalReceivedMessages = receivedMessages.total;
        $scope.criticalPathLatency = metricsProvider.getCriticalPathLatency();
      }

      $scope.$watchCollection('dag.metricsUpdateTime', function() {
        redrawGraph($scope.dag.getWeightedDagView());
        updateMetricsValues($scope.dag);
      });
    }])
;