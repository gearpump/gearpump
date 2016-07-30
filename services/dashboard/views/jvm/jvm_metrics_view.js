/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
angular.module('dashboard')

  .directive('jvmMetricsView', function () {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'views/jvm/jvm_metrics_view.html',
      scope: {
        samplingConfig: '=',
        queryMetricsFnRef: '&'
      },
      controller: ['$scope', '$filter', '$propertyTableBuilder', 'helper',
        function ($scope, $filter, $ptb, helper) {
          'use strict';

          var sc = $scope.samplingConfig;
          var recentChartPoints = sc.retainRecentDataSeconds * 1000 / sc.retainRecentDataIntervalMs;
          var histChartPoints = sc.retainHistoryDataHours * 3600 * 1000 / sc.retainHistoryDataIntervalMs;

          // property table part
          var converter = {
            bytes: function (value) {
              return {raw: value, unit: 'B', readable: true};
            },
            direct: function (value) {
              return value;
            }
          };
          var metricsClassProps = {
            'memory.total.max': ['Memory Total', converter.bytes],
            'memory.total.committed': ['Memory Committed', converter.bytes],
            'memory.total.used': ['Memory Used', converter.bytes],
            'thread.count': ['Total Thread Count', converter.direct],
            'thread.daemon.count': ['Daemon Thread Count', converter.direct]
          };

          $scope.jvmMetricsTable = _.map(metricsClassProps, function (prop) {
            var text = prop[0];
            var convertFn = prop[1];
            return $ptb.number(text).value(convertFn(0)).done();
          });

          function updateMetricsTable(metrics) {
            var updates = {};
            var i = 0;
            angular.forEach(metricsClassProps, function (prop, name) {
              if (metrics.hasOwnProperty(name)) {
                var convertFn = prop[1];
                updates[i] = convertFn(metrics[name].value);
              }
              i++;
            });
            $ptb.$update($scope.jvmMetricsTable, updates);
          }

          // metrics charts part
          $scope.isShowingCurrentMetrics = true;

          function rebuildChartsOnPeriodChanged() {
            var all = !$scope.isShowingCurrentMetrics;
            queryMetricsFn(all).then(function (metrics) {
              var dataPoints = makeMemoryUsageChartData(metrics);
              var visibleDataPointsNum = all ?
                Math.max(dataPoints.length, histChartPoints) : recentChartPoints;
              if (all && dataPoints.length < 2) {
                // Hide chart data, if there is only one data point there, which looks very ugly.
                visibleDataPointsNum = 0;
                dataPoints = [];
              }
              // Rebuild the chart
              $scope.memoryUsageChart = createMemoryUsageChart(visibleDataPointsNum, dataPoints);
            });
          }

          var queryMetricsFn = $scope.queryMetricsFnRef();
          queryMetricsFn(/*all=*/false).then(function (metrics0) {
            $scope.metrics = metrics0.$data();
            $scope.$watch('isShowingCurrentMetrics', function (newVal, oldVal) {
              if (angular.equals(newVal, oldVal)) {
                return; // ignore initial notification
              }
              rebuildChartsOnPeriodChanged();
            });
            metrics0.$subscribe($scope, function (metrics) {
              $scope.metrics = metrics;
            });
          });

          $scope.$watch('metrics', function (metrics) {
            if (angular.isObject(metrics)) {
              updateMetricsTable(_.mapValues(metrics, _.last));
              if ($scope.isShowingCurrentMetrics) {
                updateRecentMetricsCharts(metrics);
              }
            }
          });

          function updateRecentMetricsCharts(metrics) {
            $scope.memoryUsageChart.data = makeMemoryUsageChartData(metrics);
          }

          function makeMemoryUsageChartData(metrics) {
            return _.map(metrics['memory.total.used'], function (metric) {
              return {
                x: helper.timeToChartTimeLabel(metric.time, /*shortForm=*/$scope.isShowingCurrentMetrics),
                y: [metric.value]
              };
            });
          }

          function createLineChartOptions(seriesNames, visibleDataPointsNum, dataPoints) {
            return {
              height: '168px',
              margin: {right: 15},
              seriesNames: seriesNames,
              visibleDataPointsNum: visibleDataPointsNum,
              data: dataPoints,
              yAxisLabelFormatter: helper.yAxisLabelFormatterWithoutValue0('B'), // MB, GB, TB, etc.
              valueFormatter: function (value) {
                return $filter('number')(Math.floor(value / (1024 * 1024)), 0) + ' MB';
              }
            };
          }

          function createMemoryUsageChart(visibleDataPointsNum, dataPoints) {
            return {
              options: createLineChartOptions(['Memory Used'], visibleDataPointsNum, dataPoints),
              data: null
            };
          }

          $scope.memoryUsageChart = createMemoryUsageChart(recentChartPoints);
        }]
    }
  })
;
