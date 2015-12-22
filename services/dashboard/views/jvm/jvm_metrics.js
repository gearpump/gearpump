/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .directive('jvmMetrics', function() {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'views/jvm/jvm_metrics.html',
      scope: {
        samplingConfig: '=',
        queryMetricsFnRef: '&'
      },
      controller: ['$scope', '$filter', '$propertyTableBuilder', '$echarts',
        function($scope, $filter, $ptb, $echarts) {
          'use strict';

          var sc = $scope.samplingConfig;
          var currentChartPoints = sc.retainRecentDataSeconds * 1000 / sc.retainRecentDataIntervalMs;
          var histChartPoints = sc.retainHistoryDataHours * 3600 * 1000 / sc.retainHistoryDataIntervalMs;

          // property table part
          var converter = {
            bytes: function(value) {
              return {raw: value, unit: 'B', readable: true};
            },
            direct: function(value) {
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

          $scope.jvmMetricsTable = _.map(metricsClassProps, function(prop) {
            var text = prop[0];
            var convertFn = prop[1];
            return $ptb.number(text).value(convertFn(0)).done();
          });

          function updateMetricsTable(metrics) {
            var updates = {};
            var i = 0;
            angular.forEach(metricsClassProps, function(prop, clazz) {
              if (metrics.hasOwnProperty(clazz)) {
                var convertFn = prop[1];
                updates[i] = convertFn(metrics[clazz].value);
              }
              i++;
            });
            $ptb.$update($scope.jvmMetricsTable, updates);
          }

          // metrics charts part
          $scope.showCurrentMetrics = true;

          function rebuildChartsOnPeriodChanged() {
            // set null is a trick to rebuild historical charts
            $scope.memoryHistUsageChartOptions = null;

            var all = !$scope.showCurrentMetrics;
            queryMetricsFn(all).then(function(metrics) {
              if (all) {
                var options = angular.copy($scope.memoryRecentUsageChart.options);
                options.data = makeMemoryUsageChartData(metrics);
                options.visibleDataPointsNum = Math.max(options.data.length, histChartPoints);
                $scope.memoryHistUsageChartOptions = options;
              } else {
                updateRecentMetricsCharts(metrics);
              }
            });
          }

          var queryMetricsFn = $scope.queryMetricsFnRef();
          queryMetricsFn(/*all=*/false).then(function(metrics0) {
            $scope.metrics = metrics0.$data();
            $scope.$watch('showCurrentMetrics', function(newVal, oldVal) {
              if (angular.equals(newVal, oldVal)) {
                return; // ignore initial notification
              }
              rebuildChartsOnPeriodChanged();
            });
            metrics0.$subscribe($scope, function(metrics) {
              $scope.metrics = metrics;
            });
          });

          $scope.$watch('metrics', function(metrics) {
            if (angular.isObject(metrics)) {
              updateMetricsTable(_.mapValues(metrics, _.last));
              updateRecentMetricsCharts(metrics);
            }
          });

          function updateRecentMetricsCharts(metrics) {
            $scope.memoryRecentUsageChart.data = makeMemoryUsageChartData(metrics);
          }

          function makeMemoryUsageChartData(metrics) {
            return _.map(metrics['memory.total.used'], function(metric) {
              return {
                x: moment(metric.time).format('HH:mm:ss'),
                y: [metric.value]
              };
            });
          }

          var lineChartOptionBase = {
            height: '168px',
            visibleDataPointsNum: currentChartPoints,
            data: _.times(currentChartPoints, function() {
              return {x: '', y: '-'};
            })
          };

          $scope.memoryRecentUsageChart = {
            options: angular.merge({
              seriesNames: ['Memory Used'],
              yAxisLabelFormatter: $echarts.axisLabelFormatter('B'), // MB, GB, TB, etc.
              valueFormatter: function(value) {
                return $filter('number')(Math.floor(value / (1024 * 1024)), 0) + ' MB';
              }
            }, lineChartOptionBase)
          };
        }]
    }
  })
;