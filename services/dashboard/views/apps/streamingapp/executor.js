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
            modelExecutor: ['$stateParams', 'models', function($stateParams, models) {
              return models.$get.app.executor($stateParams.appId, $stateParams.executorId);
            }],
            modelMetrics: ['$stateParams', 'models', function($stateParams, models) {
              return models.$get.app.executorMetrics(
                $stateParams.appId, $stateParams.executorId, /*all=*/true);
            }],
          }
        });
    }])

  .controller('StreamingAppExecutorCtrl', ['$scope', '$stateParams', '$propertyTableBuilder', 'restapi', 'modelExecutor', 'modelMetrics',
    function($scope, $stateParams, $ptb, restapi, modelExecutor, modelMetrics) {
      'use strict';

      $scope.overviewTable = [
        $ptb.text('ID').done(),
        $ptb.text('Actor Path').done(),
        $ptb.link('Worker').done(),
        $ptb.number('Task Count').done(),
        $ptb.button('Quick Links').done()
      ];

      function updateOverviewTable(executor) {
        angular.merge($scope.overviewTable, [
          {value: executor.id},
          {value: executor.actorPath},
          {value: {href: executor.workerPageUrl, text: 'Worker ' + executor.workerId}},
          {value: executor.taskCount},
          {values: [
              {href: restapi.appExecutorConfigLink($scope.app.appId, executor.id), text: 'Config', class: 'btn-xs'},
              {tooltip: executor.logFile, text: 'Log Dir.', class: 'btn-xs'}
          ]}
        ]);
      }

      $scope.executor = modelExecutor.$data();
      $scope.metrics = modelMetrics.$data();

      updateOverviewTable($scope.executor);
      modelExecutor.$subscribe($scope, function(executor) {
        $scope.executor = executor;
        updateOverviewTable(executor);
      });

      modelMetrics.$subscribe($scope, function(metrics) {
        $scope.metrics = metrics;
      });
    }])
;
