/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .config(['$stateProvider',
    function($stateProvider) {
      'use strict';

      $stateProvider
        .state('streamingapp', {
          abstract: true,
          url: '/apps/streamingapp/:appId',
          templateUrl: 'views/apps/streamingapp/streamingapp.html',
          controller: 'StreamingAppCtrl',
          resolve: {
            app0: ['$stateParams', 'models', function($stateParams, models) {
              return models.$get.app($stateParams.appId);
            }],
            metrics0: ['$stateParams', 'models', function($stateParams, models) {
              return models.$get.appMetrics($stateParams.appId, /*all=*/true);
            }]
          }
        });
    }])

/**
 * This controller is used to obtain app. All nested views will read status from here.
 */
  .controller('StreamingAppCtrl', ['$scope', '$state', 'conf', 'app0', 'metrics0', 'models',
    function($scope, $state, conf, app0, metrics0, models) {
      'use strict';

      $scope.$state = $state; // required by streamingapp.html
      $scope.app = app0;
      $scope.dag = models.createDag(app0.clock, app0.processors,
        app0.processorLevels, app0.dag.edgeList, app0.executors);

      app0.$subscribe($scope, function(app) {
        $scope.app = app;
        $scope.dag.setData(app.clock, app.processors,
          app.processorLevels, app.dag.edgeList);
      });

      models.$get.appStallingProcessors(app0.appId)
        .then(function(processors) {
          $scope.dag.setStallingTasks(Object.keys(processors.$data()));
          processors.$subscribe($scope, function(data) {
            $scope.dag.setStallingTasks(Object.keys(data));
          });
        });

      $scope.dag.updateMetricsArray(metrics0.$data());
      metrics0.$subscribe($scope, function(metrics) {
        $scope.dag.updateMetricsArray(metrics);
      });
    }])
;