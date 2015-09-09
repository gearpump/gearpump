/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .controller('JvmMetricsChartsCtrl', ['$scope', '$filter', '$propertyTableBuilder', '$echarts', 'sorted', 'conf',
    function($scope, $filter, $ptb, $echarts, sorted, conf) {
      'use strict';

      $scope.jvmMetricsTable = [
        $ptb.bytes('Memory Total').done(),
        $ptb.bytes('Memory Committed').done(),
        $ptb.bytes('Memory Used').done(),
        $ptb.number('Total Thread Count').done(),
        $ptb.number('Daemon Thread Count').done()
      ];

      function updateMetricsTable(metrics) {
        $ptb.$update($scope.jvmMetricsTable, [
          {raw: metrics['memory.total.max'].value, unit: 'B', readable: true},
          {raw: metrics['memory.total.committed'].value, unit: 'B', readable: true},
          {raw: metrics['memory.total.used'].value, unit: 'B', readable: true},
          metrics['thread.count'].value,
          metrics['thread.daemon.count'].value
        ]);
      }

      function updateMetricsCharts(metrics) {
        $scope.chartData = buildChartData([
          metrics['memory.total.used']
        ]);
      }

      function buildChartData(metricsArray) {
        var data = {};
        _.forEach(metricsArray, function(metricArray, clazz) {
          _.forEach(metricArray, function(metric) {
            data[metric.time] = data[metric.time] || {};
            data[metric.time][clazz] = metric.value;
          });
        });

        return _.map(sorted.byKey(data), function(datum, timeString) {
          return {
            x: moment(Number(timeString)).format('HH:mm:ss'),
            y: _.values(datum)
          };
        });
      }

      $scope.$watch('metrics', function(metrics) {
        updateMetricsTable(metrics);
      });

      $scope.$watch('historicalMetrics', function(metrics) {
        updateMetricsCharts(metrics);
      });

      var lineChartOptionBase = {
        height: '168px',
        seriesStacked: false,
        visibleDataPointsNum: conf.metricsChartDataCount,
        yAxisLabelFormatter: $echarts.axisLabelFormatter('B')
      };

      $scope.chartOptions = angular.merge({
        seriesNames: ['Memory Used'],
        valueFormatter: function(value) {
          return $filter('number')(Math.floor(value / (1024 * 1024)), 0) + ' MB';
        }
      }, lineChartOptionBase);
    }])
;