/*
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
angular.module('dashboard')

  .config(['$stateProvider',
    function ($stateProvider) {
      'use strict';

      $stateProvider
        .state('streamingapp', {
          abstract: true,
          url: '/apps/streamingapp/:appId',
          templateUrl: 'views/apps/streamingapp/streamingapp.html',
          controller: 'StreamingAppCtrl',
          resolve: {
            app0: ['$stateParams', 'models', function ($stateParams, models) {
              return models.$get.app($stateParams.appId);
            }]
          }
        });
    }])

/**
 * This controller is used to obtain app. All nested views will read status from here.
 */
  .controller('StreamingAppCtrl', ['$scope', '$state', 'i18n', 'helper', 'models', 'app0',
    function ($scope, $state, i18n, helper, models, app0) {
      'use strict';

      $scope.whatIsAppClock = i18n.terminology.appClock;
      $scope.$state = $state; // required by streamingapp.html
      $scope.app = app0;
      $scope.metricsConfig = app0.historyMetricsConfig;
      $scope.uptimeCompact = helper.readableDuration(app0.uptime);
      $scope.dag = models.createDag(app0.clock, app0.processors, app0.dag.edgeList);

      app0.$subscribe($scope, function (app) {
        updateAppDetails(app);
      }, /*onerror=*/ function () {
        // manually reset status fields on an error response
        var app = angular.copy($scope.app);
        app.status = 'terminated';
        app.isRunning = false;
        _.forEach(app.executors, function (executor) {
          executor.status = 'terminated';
          executor.isRunning = false;
        });
        updateAppDetails(app);
      });

      function updateAppDetails(app) {
        $scope.app = app;
        $scope.uptimeCompact = helper.readableDuration(app.uptime);
        $scope.dag.setData(app.clock, app.processors, app.dag.edgeList);
      }

      models.$get.appStallingTasks(app0.appId)
        .then(function (tasks0) {
          updateStallingTasks(tasks0.$data());
          tasks0.$subscribe($scope, function (tasks) {
            updateStallingTasks(tasks);
          });
        });
      function updateStallingTasks(tasks) {
        $scope.appClockConcern = ($scope.app.isRunning && Object.keys(tasks).length) ?
          "Application clock does not go forward. Click here to check red processor(s)." : undefined;
        $scope.dag.setStallingTasks(tasks);
      }

      $scope.switchToDagTab = function () {
        $state.go('streamingapp.dag');
      };

      // As metrics will be collected shortly after application is started, we should not receive initial metrics
      // at the view resolving stage.
      models.$subscribe($scope,
        function () {
          return models.$get.appLatestMetrics(app0.appId);
        },
        function (metrics0) {
          $scope.metrics = metrics0.$data();
          metrics0.$subscribe($scope, function (metrics) {
            $scope.metrics = metrics;
          });
        });
      $scope.$watch('metrics', function (metrics) {
        if (angular.isObject(metrics)) {
          $scope.dag.updateLatestMetrics(metrics);
        }
      });

      // Angular template cannot call the function directly, so export a function.
      $scope.size = function (obj) {
        return Object.keys(obj).length;
      };

      $scope.sendThroughputMetricsCaption = 'Source Processors Send Throughput';
      $scope.sendThroughputMetricsDescription = 'Messages are sent from source processors';
      $scope.receiveThroughputMetricsCaption = 'Sink Processors Receive Throughput';
      $scope.receiveThroughputMetricsDescription = 'Messages are received by sink processors';
      $scope.messageLatencyMetricsCaption = 'End-to-End Latency';
      $scope.messageLatencyMetricsDescription = 'The largest latency from a source processor to a sink processor. The value is the sum of message receive latency plus message processing time of all processors on the path (except the source processor).';
      $scope.averageMessageProcessingTimeMetricsCaption = 'Average Message Processing Time';
      $scope.averageMessageProcessingTimeMetricsDescription = 'The processing time is the duration from a message is received by a processor and to the message is sent to the next stop';
    }])
;
