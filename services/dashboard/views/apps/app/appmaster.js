/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.apps.appmaster', ['directive.visgraph', 'dashboard.streamingdag'])

  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider
      .when('/apps/app/:id', {
        label: 'Application ',
        templateUrl: 'views/apps/app/appmaster.html',
        controller: 'AppMasterCtrl'
      });
  }])

  .controller('AppMasterCtrl',
  ['$scope', '$routeParams', 'breadcrumbs', 'restapi', 'StreamingService', 'visdagUtil', 'StreamingDag',
    function ($scope, $routeParams, breadcrumbs, restapi, StreamingService, visdagUtil, StreamingDag) {
      $scope.app = {id: $routeParams.id};
      breadcrumbs.options = {'Application ': 'Application ' + $scope.app.id};

      $scope.streamingDag = null;
      restapi.subscribe('/appmaster/' + $scope.app.id + '?detail=true', $scope,
        function (data) {
          // TODO: Serde GeneralAppMasterDataDetail (#458)
          if (data.hasOwnProperty('appName')) {
            $scope.app = {
              actorPath: data.actorPath,
              duration: data.clock,
              executors: data.executors,
              id: data.appId,
              name: data.appName
            };
          }

          // TODO: Serde Dag (#458)
          if (data.hasOwnProperty('dag') && data.hasOwnProperty('processors')) {
            if ($scope.streamingDag === null) {
              $scope.streamingDag = new StreamingDag($scope.app.id, data.processors,
                data.processorLevels, data.dag.edges);
              $scope.visgraph = {
                options: visdagUtil.newOptions(),
                data: visdagUtil.newData()
              };

              // Usually metrics will be pushed by websocket. In worst case, metrics might be available
              // in couple of seconds. This will cause some charts to be empty. For better user experience,
              // we will manually fetch metrics via restapi at least once, before websocket is ready.
              if ($scope.streamingDag.lastUpdated === null) {
                restapi.repeatUntil('/metrics/app/' + $scope.app.id + '/app' + $scope.app.id, $scope,
                  function (data) {
                    // TODO: Serde HistoryMetrics (#458)
                    if ($scope.streamingDag.lastUpdated === null && data !== null) {
                      $scope.streamingDag.updateMetricsArray(data.metrics);
                      $scope.redrawVisGraph();
                    }
                    return $scope.streamingDag.lastUpdated !== null;
                  });
              }
            }
            $scope.redrawVisGraph();
          }
        });

      /** Redraw VisGraph on demand */
      $scope.redrawVisGraph = function() {
        $scope.streamingDag.updateVisGraphNodes($scope.visgraph.data.nodes, [2, 16]);
        $scope.streamingDag.updateVisGraphEdges($scope.visgraph.data.edges, [0.5, 4], [0.5, 0.1]);
      };

      var request = JSON.stringify(
        ["org.apache.gearpump.cluster.MasterToAppMaster.AppMasterMetricsRequest",
          {appId: parseInt($scope.app.id)}
        ]);
      StreamingService.subscribe(request, $scope, function (event) {
        // TODO: Serde Metrics (#458)
        var obj = angular.fromJson(event.data);
        $scope.streamingDag.updateMetrics(obj[0], obj[1]);
      });
    }])

  .factory('visdagUtil', function () {
    var fontFace = "'lato', 'helvetica neue', 'segoe ui', arial";
    return {
      newOptions: function () {
        return {
          width: '100%',
          height: '100%',
          hierarchicalLayout: {
            layout: 'direction',
            direction: "UD"
          },
          stabilize: true /* stabilize positions before displaying */,
          freezeForStabilization: true,
          nodes: {
            shape: 'dot',
            fontSize: 12,
            fontFace: fontFace,
            fontStrokeColor: '#fff',
            fontStrokeWidth: 2
          },
          edges: {
            style: 'arrow',
            labelAlignment: 'line-center',
            fontSize: 11,
            fontFace: fontFace
          }
        };
      },

      newData: function () {
        return {
          nodes: new vis.DataSet(),
          edges: new vis.DataSet()
        };
      }
    };
  })
;
