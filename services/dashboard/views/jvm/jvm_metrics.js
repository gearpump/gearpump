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
        queryMetricsFnRef: '&'
      },
      controller: ['$scope', '$filter', '$propertyTableBuilder', '$echarts', 'conf',
        function($scope, $filter, $ptb, $echarts, conf) {
          'use strict';

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
            return $ptb.number(text).done();
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
          $scope.options = {'current': 'Current', 'hist': 'Past 48 Hours'};
          $scope.period = 'current';

          function rebuildChartsOnPeriodChanged(period) {
            // set null is a trick to rebuild historical charts
            $scope.memoryHistUsageChartOptions = null;

            var all = 'hist' === String(period);
            queryMetricsFn(all).then(function(metrics) {
              if (all) {
                var options = angular.copy($scope.memoryRecentUsageChart.options);
                options.data = makeMemoryUsageChartData(metrics);
                options.visibleDataPointsNum = Math.max(options.data.length, conf.metricsDataPointsP48H);
                $scope.memoryHistUsageChartOptions = options;
              } else {
                updateRecentMetricsCharts(metrics);
              }
            });
          }

          var queryMetricsFn = $scope.queryMetricsFnRef();
          queryMetricsFn(/*all=*/false).then(function(metrics0) {
            $scope.metrics = metrics0.$data();
            $scope.$watch('period', rebuildChartsOnPeriodChanged);
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
            visibleDataPointsNum: conf.metricsDataPointsP5M,
            data: _.times(conf.metricsDataPointsP5M, function() {
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