/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .config(['$stateProvider',
    function($stateProvider) {
      'use strict';

      $stateProvider
        .state('streamingapp.executor', {
          url: '/executor/:executorId',
          templateUrl: 'views/apps/streamingapp/executor.html',
          controller: 'StreamingAppExecutorCtrl',
          resolve: {
            executor0: ['$stateParams', 'models', function($stateParams, models) {
              return models.$get.appExecutor($stateParams.appId, $stateParams.executorId);
            }],
            metrics0: ['$stateParams', 'models', function($stateParams, models) {
              return models.$get.appExecutorMetrics(
                $stateParams.appId, $stateParams.executorId, /*all=*/true);
            }],
          }
        });
    }])

  .controller('StreamingAppExecutorCtrl', ['$scope', '$stateParams', '$propertyTableBuilder', 'restapi', 'executor0', 'metrics0',
    function($scope, $stateParams, $ptb, restapi, executor0, metrics0) {
      'use strict';

      $scope.overviewTable = [
        $ptb.text('ID').done(),
        $ptb.text('Actor Path').done(),
        $ptb.link('Worker').done(),
        $ptb.number('Task Count').done(),
        $ptb.button('Quick Links').values([
            {href: restapi.appExecutorConfigLink($scope.app.appId, executor0.id), text: 'Config', class: 'btn-xs'},
            {tooltip: executor0.logFile, text: 'Log Dir.', class: 'btn-xs'}
          ]
        ).done()
      ];

      function updateOverviewTable(executor) {
        $ptb.$update($scope.overviewTable, [
          executor.id,
          executor.actorPath,
          {href: executor.workerPageUrl, text: 'Worker ' + executor.workerId},
          executor.taskCount
          /* placeholder for quick links, but they do not need to be updated */
        ]);
      }

      $scope.executor = executor0.$data();
      updateOverviewTable($scope.executor);
      executor0.$subscribe($scope, function(executor) {
        $scope.executor = executor;
        updateOverviewTable(executor);
      });

      // JvmMetricsChartsCtrl will watch `$scope.metrics`
      $scope.metrics = metrics0.$data();
      metrics0.$subscribe($scope, function(metrics) {
        $scope.metrics = metrics;
      });
    }])
;
