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

  .controller('AppMasterCtrl', ['$scope', '$routeParams', 'breadcrumbs', 'restapi', 'StreamingService', 'dagStyle', 'StreamingDag',
    function ($scope, $routeParams, breadcrumbs, restapi, StreamingService, dagStyle, StreamingDag) {
      $scope.app = {id: $routeParams.id};
      breadcrumbs.options = {'Application ': 'Application ' + $scope.app.id};

      $scope.streamingDag = null;
      restapi.subscribe('/appmaster/' + $scope.app.id + '?detail=true', $scope,
        function (data) {
          // TODO: Serde GeneralAppMasterDataDetail (#458)
          if (data.hasOwnProperty('appName')) {
            $scope.app = {
              actorPath: data.actorPath,
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
              var depth = data.processorLevels.reduce(function(total, b) {
                return Math.max(total, b[1]);
              }, 0);
              $scope.visgraph = {
                options: dagStyle.newOptions({depth: depth}),
                data: dagStyle.newData()
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
        $scope.streamingDag.updateVisGraphNodes($scope.visgraph.data.nodes, dagStyle.nodeRadiusRange());
        $scope.streamingDag.updateVisGraphEdges($scope.visgraph.data.edges, dagStyle.edgeWidthRange(), dagStyle.edgeArrowSizeRange());
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
;
