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
            }]
          }
        });
    }])

  .controller('StreamingAppExecutorCtrl', ['$scope', '$stateParams', '$propertyTableBuilder',
    'helper', 'restapi', 'conf', 'models', 'executor0',
    function($scope, $stateParams, $ptb, helper, restapi, conf, models, executor0) {
      'use strict';

      $scope.executorName = executor0.id === -1 ?
        'AppMaster' : ('Executor ' + executor0.id);
      $scope.overviewTable = [
        $ptb.text('JVM Info').help('Format: PID@hostname').done(),
        $ptb.text('Actor Path').done(),
        $ptb.link('Worker').done(),
        $ptb.number('Task Count').done(),
        $ptb.button('Quick Links').values([
            {href: restapi.appExecutorConfigLink($scope.app.appId, executor0.id), target: '_blank', text: 'Config', class: 'btn-xs'},
            helper.withClickToCopy({text: 'Log Dir.', class: 'btn-xs'}, executor0.logFile)
          ]
        ).done()
      ];

      function updateOverviewTable(executor) {
        $ptb.$update($scope.overviewTable, [
          executor.jvmName,
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
      $scope.$on('$destroy', function() {
        $scope.destroyed = true;
      });
      models.$subscribe($scope,
        function() {
          return models.$get.appExecutorMetrics($scope.app.appId, executor0.id);
        },
        function(metrics0) {
          $scope.metrics = metrics0.$data();
          metrics0.$subscribe($scope, function(metrics) {
            $scope.metrics = metrics;
          });
        });
      models.$subscribe($scope,
        function() {
          return models.$get.appExecutorHistoricalMetrics($scope.app.appId, executor0.id,
            conf.metricsChartSamplingRate, conf.metricsChartDataCount);
        },
        function(historicalMetrics0) {
          $scope.historicalMetrics = historicalMetrics0.$data();
          historicalMetrics0.$subscribe($scope, function(metrics) {
            $scope.historicalMetrics = metrics;
          });
        });
    }])
;