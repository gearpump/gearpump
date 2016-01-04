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
            }]
          }
        });
    }])

/**
 * This controller is used to obtain app. All nested views will read status from here.
 */
  .controller('StreamingAppCtrl', ['$scope', '$state', 'helper', 'models', 'app0',
    function($scope, $state, helper, models, app0) {
      'use strict';

      $scope.$state = $state; // required by streamingapp.html
      $scope.app = app0;
      $scope.metricsConfig = app0.historyMetricsConfig;
      $scope.uptimeCompact = helper.readableDuration(app0.uptime);
      $scope.dag = models.createDag(app0.clock, app0.processors,
        app0.processorLevels, app0.dag.edgeList);

      app0.$subscribe($scope, function(app) {
        updateAppDetails(app);
      }, /*onerror=*/ function() {
        // manually reset status fields on an error response
        var app = angular.copy($scope.app);
        app.status = 'terminated';
        app.isRunning = false;
        _.forEach(app.executors, function(executor) {
          executor.status = 'terminated';
          executor.isRunning = false;
        });
        updateAppDetails(app);
      });

      function updateAppDetails(app) {
        $scope.app = app;
        $scope.uptimeCompact = helper.readableDuration(app.uptime);
        $scope.dag.setData(app.clock, app.processors,
          app.processorLevels, app.dag.edgeList);
      }

      models.$get.appStallingTasks(app0.appId)
        .then(function(tasks0) {
          updateStallingTasks(tasks0.$data());
          tasks0.$subscribe($scope, function(tasks) {
            updateStallingTasks(tasks);
          });
        });
      function updateStallingTasks(tasks) {
        $scope.appClockConcern = ($scope.app.isRunning && Object.keys(tasks).length) ?
          "Application clock does not go forward. Click here to check red processor(s)." : undefined;
        $scope.dag.setStallingTasks(tasks);
      }
      $scope.switchToDagTab = function() {
        $state.go('streamingapp.dag');
      };

      // As metrics will be collected shortly after application is started, we should not receive initial metrics
      // at the view resolving stage.
      models.$subscribe($scope,
        function() {
          return models.$get.appLatestMetrics(app0.appId);
        },
        function(metrics0) {
          $scope.metrics = metrics0.$data();
          metrics0.$subscribe($scope, function(metrics) {
            $scope.metrics = metrics;
          });
        });
      $scope.$watch('metrics', function(metrics) {
        if (angular.isObject(metrics)) {
          $scope.dag.updateMetrics(metrics);
        }
      });

      // Angular template cannot call the function directly, so export a function.
      $scope.size = function(obj) {
        return Object.keys(obj).length;
      };

      $scope.sendThroughputMetricsCaption = 'Source Processors Send Throughput';
      $scope.sendThroughputMetricsDescription = 'Messages are sent from external data source';
      $scope.receiveThroughputMetricsCaption = 'Sink Processors Receive Throughput';
      $scope.receiveThroughputMetricsDescription = 'Messages are received by external data sink';
      $scope.averageMesssageProcessingTimeMetricsCaption = 'Average Message Processing Time';
      $scope.averageMesssageProcessingTimeMetricsDescription = 'Processing time is the duration from a message is received to the message is sent';
      $scope.messageReceiveLatencyMetricsCaption = 'Effective Message Receive Latency';
      $scope.messageReceiveLatencyMetricsDescription = 'Effective receive latency is the duration from a message is received from the source processor to the message is sent by the sink processor along the longest path';
    }])
;