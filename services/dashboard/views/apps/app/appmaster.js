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

  .controller('AppMasterCtrl', ['$scope', '$routeParams', 'breadcrumbs', 'restapi', 'StreamingService', 'conf', 'StreamingDag',
    function ($scope, $routeParams, breadcrumbs, restapi, StreamingService, conf, StreamingDag) {
      $scope.tabs = [
        {heading: 'Status', templateUrl: 'views/apps/app/appstatus.html', controller: 'AppStatusCtrl'},
        {heading: 'DAG', templateUrl: 'views/apps/app/appdag.html', controller: 'AppDagCtrl'},
        {heading: 'Processor', templateUrl: 'views/apps/app/appprocessor.html', controller: 'AppProcessorCtrl'},
        {heading: 'Metrics', templateUrl: 'views/apps/app/appmetrics.html', controller: 'AppMetricsCtrl'}
      ];

      $scope.app = {id: $routeParams.id};
      breadcrumbs.options = {'Application ': 'Application' + $scope.app.id};

      $scope.streamingDag = null;
      restapi.subscribe('/appmaster/' + $scope.app.id + '?detail=true', $scope,
        function (data) {
          // TODO: Serde GeneralAppMasterDataDetail (#458)
          if (data.hasOwnProperty('appName')) {
            $scope.app = {
              actorPath: data.actorPath,
              clock: data.clock,
              executors: data.executors,
              id: data.appId,
              name: data.appName
            };

            breadcrumbs.options = {'Application ': 'Application' + $scope.app.id + ' (' + data.appName + ')'};
          }

          // TODO: Serde Dag (#458)
          if (data.hasOwnProperty('dag')) {
            if (!$scope.streamingDag) {
              $scope.streamingDag = new StreamingDag($scope.app.id, $scope.app.clock,
                data.processors, data.processorLevels, data.dag.edgeList, data.executors);

              // Usually metrics will be pushed by websocket. In worst case, metrics might be available
              // in couple of seconds. This will cause some charts to be empty. For better user experience,
              // we will manually fetch metrics via restapi at least once, before websocket is ready.
              if (!$scope.streamingDag.hasMetrics()) {
                var url = '/appmaster/' + $scope.app.id + '/metrics/app' + $scope.app.id +'?readLatest=true';
                restapi.repeatUntil(url, $scope, function (data) {
                  // TODO: Serde HistoryMetrics (#458)
                  if (data !== null &&
                    (!conf.webSocketPreferred || !$scope.streamingDag.hasMetrics())) {
                    $scope.streamingDag.updateMetricsArray(data.metrics);
                  }
                  return conf.webSocketPreferred && $scope.streamingDag.hasMetrics();
                });
              }
            } else {
              $scope.streamingDag.setData($scope.app.clock, data.processors, data.processorLevels,
                data.dag.edgeList, data.executors);
            }
          }
        });

      if (conf.webSocketPreferred) {
        var request = JSON.stringify(
          ["org.apache.gearpump.cluster.MasterToAppMaster.AppMasterMetricsRequest",
            {appId: parseInt($scope.app.id)}
          ]);
        StreamingService.subscribe(request, $scope, function (event) {
          // TODO: Serde Metrics (#458)
          var obj = angular.fromJson(event.data);
          $scope.streamingDag.updateMetrics(obj[0], obj[1]);
        });
      }

      $scope.switchToTaskTab = function (processorId) {
        $scope.activeProcessorId = processorId;
        $scope.switchToTabIndex = {tabIndex: 2, reload: true};
      };
    }])

  .filter('lastPart', function () {
    return function lastPart(name) {
      if (name) {
        var parts = name.split('.');
        if (parts.length > 0) {
          return parts[parts.length - 1];
        }
      }
    };
  })
;
