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
                $stateParams.appId, $stateParams.executorId);
            }],
            historicalMetrics0: ['$stateParams', 'models', 'conf', function($stateParams, models, conf) {
              return models.$get.appExecutorHistoricalMetrics(
                $stateParams.appId, $stateParams.executorId,
                conf.metricsChartSamplingRate, conf.metricsChartDataCount);
            }]
          }
        });
    }])

  .controller('StreamingAppExecutorCtrl', ['$scope', '$stateParams', '$propertyTableBuilder',
    'helper', 'restapi', 'executor0', 'metrics0', 'historicalMetrics0',
    function($scope, $stateParams, $ptb, helper, restapi, executor0, metrics0, historicalMetrics0) {
      'use strict';

      $scope.overviewTable = [
        $ptb.text('ID').done(),
        $ptb.text('Actor Path').done(),
        $ptb.link('Worker').done(),
        $ptb.number('Task Count').done(),
        $ptb.button('Quick Links').values([
            {href: restapi.appExecutorConfigLink($scope.app.appId, executor0.id), text: 'Config', class: 'btn-xs'},
            helper.withClickToCopy({text: 'Log Dir.', class: 'btn-xs'}, executor0.logFile)
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

      // JvmMetricsChartsCtrl will watch `$scope.metrics` and `$scope.historicalMetrics`.
      $scope.metrics = metrics0.$data();
      metrics0.$subscribe($scope, function(metrics) {
        $scope.metrics = metrics;
      });
      $scope.historicalMetrics = historicalMetrics0.$data();
      historicalMetrics0.$subscribe($scope, function(metrics) {
        $scope.historicalMetrics = metrics;
      });
    }])
;