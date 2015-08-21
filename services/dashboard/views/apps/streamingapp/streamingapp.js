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
            modelApp: ['$stateParams', 'models', function($stateParams, models) {
              return models.$get.app.detail($stateParams.appId);
            }],
            modelMetrics: ['$stateParams', 'models', function($stateParams, models) {
              return models.$get.app.metrics($stateParams.appId, /*all=*/true);
            }]
          }
        });
    }])

/**
 * This controller is used to obtain app. All nested views will read status from here.
 */
  .controller('StreamingAppCtrl', ['$scope', '$state', 'conf', 'modelApp', 'modelMetrics', 'models',
    function($scope, $state, conf, modelApp, modelMetrics, models) {
      'use strict';

      $scope.$state = $state; // required by streamingapp.html
      $scope.app = modelApp;
      $scope.dag = models.createDag(modelApp.clock, modelApp.processors,
        modelApp.processorLevels, modelApp.dag.edgeList, modelApp.executors);

      modelApp.$subscribe($scope, function(app) {
        $scope.app = app;
        $scope.dag.setData(app.clock, app.processors,
          app.processorLevels, app.dag.edgeList);
      });

      models.$get.app.stallingProcessors(modelApp.appId)
        .then(function(processors) {
          $scope.dag.setStallingTasks(Object.keys(processors.$data()));
          processors.$subscribe($scope, function(data) {
            $scope.dag.setStallingTasks(Object.keys(data));
          });
        });

      $scope.dag.updateMetricsArray(modelMetrics.$data());
      modelMetrics.$subscribe($scope, function(metrics) {
        $scope.dag.updateMetricsArray(metrics);
      });
    }])
;