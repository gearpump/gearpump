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
            }]
          }
        });
    }])

/**
 * This controller is used to obtain app. All nested views will read status from here.
 */
  .controller('StreamingAppCtrl', ['$scope', '$state', 'conf', 'modelApp', 'models',
    function($scope, $state, conf, modelApp, models) {
      'use strict';

      $scope.$state = $state; // required by streamingapp.html
      $scope.app = modelApp;
      $scope.dag = models.createDag(modelApp.clock, modelApp.processors,
        modelApp.processorLevels, modelApp.dag.edgeList, modelApp.executors);

      modelApp.$subscribe($scope, function(app) {
        $scope.app = app;
        $scope.dag.setData(modelApp.clock, modelApp.processors,
          modelApp.processorLevels, modelApp.dag.edgeList);
      });

      models.$get.app.stallingProcessors(modelApp.appId)
        .then(function(processors) {
          $scope.dag.setStallingTasks(Object.keys(processors.$data()));
          processors.$subscribe($scope, function(data) {
            $scope.dag.setStallingTasks(Object.keys(data));
          });
        });

      // TODO: Retrieving metrics is not plausible, so we load metrics deferred.
      // TODO: Disable metrics related tabs while metrics are not available?
      models.$get.app.metrics(modelApp.appId, /*all=*/true)
        .then(function(metrics) {
          $scope.dag.updateMetricsArray(metrics.$data());
          metrics.$subscribe($scope, function(data) {
            $scope.dag.updateMetricsArray(data);
          });
        });
    }])
;
