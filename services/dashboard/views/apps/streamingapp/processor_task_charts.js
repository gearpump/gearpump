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

  // todo: refactoring required
  .controller('StreamingAppProcessorTaskChartsCtrl', ['$scope', 'helper', 'models',
    function ($scope, helper, models) {
      'use strict';

      // rebuild the charts when `tasks` is changed.
      $scope.$watchCollection('tasks', function (tasks) {
        if (!tasks.selected) {
          console.warn('should not load this page');
          return;
        }
        // todo: For the time being we can only query one task
        $scope.selectedTaskIds = _.map(tasks.selected, function (taskName) {
          return Number(taskName.substr(1));
        });

        var range = {start: $scope.selectedTaskIds[0], stop: $scope.selectedTaskIds[0]};
        models.$get.appTaskLatestMetricValues(
          $scope.app.appId, $scope.processor.id, /*metricName=*/'', range).then(function (metrics0) {
            $scope.selectedMetrics = metrics0.$data();
            metrics0.$subscribe($scope, function (metrics) {
              $scope.selectedMetrics = metrics;
            });
          });

        var sc = $scope.metricsConfig;
        var recentChartPoints = sc.retainRecentDataSeconds * 1000 / sc.retainRecentDataIntervalMs;
        var lineChartOptionBase = {
          height: '108px',
          margin: {right: 15},
          visibleDataPointsNum: recentChartPoints,
          data: _.times(recentChartPoints, function () {
            return {x: '', y: '-'};
          }),
          seriesNames: tasks.selected
        };

        var throughputChartOptions = angular.merge({
          valueFormatter: function (value) {
            return helper.readableMetricValue(value) + ' msg/s';
          }
        }, lineChartOptionBase);

        var durationChartOptions = angular.merge({
          valueFormatter: function (value) {
            return helper.readableMetricValue(value) + ' ms';
          }
        }, lineChartOptionBase);

        $scope.receiveMessageRateChart = {
          options: throughputChartOptions
        };
        $scope.sendMessageRateChart = {
          options: throughputChartOptions
        };
        $scope.processingTimeChart = {
          options: durationChartOptions
        };
        $scope.receiveLatencyChart = {
          options: durationChartOptions
        };

        $scope.$watchCollection('selectedMetrics', function (metrics) {
          if (angular.isObject(metrics)) {
            updateMetricsCharts(metrics);
          }
        });

        function updateMetricsCharts(metrics) {
          var timeLabel = moment().format('HH:mm:ss');
          $scope.receiveMessageRateChart.data = [{
            x: timeLabel,
            y: extractMessageThroughput(metrics.receiveThroughput)
          }];
          $scope.sendMessageRateChart.data = [{
            x: timeLabel,
            y: extractMessageThroughput(metrics.sendThroughput)
          }];
          $scope.processingTimeChart.data = [{
            x: timeLabel,
            y: extractTimeAverage(metrics.processTime)
          }];
          $scope.receiveLatencyChart.data = [{
            x: timeLabel,
            y: extractTimeAverage(metrics.receiveLatency)
          }];
        }

        function extractMessageThroughput(metrics) {
          return extractSelectedMetricField(metrics, 'meanRate');
        }

        function extractTimeAverage(metrics) {
          return extractSelectedMetricField(metrics, 'mean');
        }

        function extractSelectedMetricField(metrics, field) {
          return _.map($scope.selectedTaskIds, function (taskId) {
            return helper.metricRounded(metrics[taskId][field]);
          });
        }
      });
    }])
;
