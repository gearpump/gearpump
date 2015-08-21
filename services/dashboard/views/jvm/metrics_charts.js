/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .controller('JvmMetricsChartsCtrl', ['$scope', '$propertyTableBuilder', 'conf',
    function($scope, $ptb, conf) {
      'use strict';

      $scope.jvmMetricsTable = [
        $ptb.number('Memory Total').done(),
        $ptb.number('Memory Used').done(),
        $ptb.number('Thread Count').done(),
        $ptb.number('Thread Daemon Count').done()
      ];

      function updateMetricsTable(metrics) {
        angular.merge($scope.jvmMetricsTable, [
          {value: {number: _.last(metrics['memory.total.max']).value / 1024 / 1024, unit: 'MB'}},
          {value: {number: _.last(metrics['memory.total.used']).value / 1024 / 1024, unit: 'MB'}},
          {value: _.last(metrics['thread.count']).value},
          {value: _.last(metrics['thread.daemon.count']).value}
        ]);
      }

      function updateMetricsCharts(metrics) {
        var array = metrics['memory.total.used'];
        $scope.chartData = _.map(array, function(metric) {
          return {x: moment.unix(metric.time).format('HH:mm:ss'), y: metric.value};
        });
      }

      $scope.$watch('metrics', function(metrics) {
        updateMetricsTable(metrics);
        updateMetricsCharts(metrics);
      });

      var initialData = _.map(
        $scope.metrics['memory.total.used'],
        function(metric) {
          return {x: moment.unix(metric.time).format('HH:mm:ss'), y: metric.value};
        });
      var sparkLineOptionBase = {
        height: '108px',
        data: initialData,
        maxDataNum: conf.metricsChartDataCount,
        yAxisLabelFormatter: function(value) {
          return Math.floor(value / 1024 / 1024) + ' MB';
        },
        seriesNames: ['T1']
      };

      $scope.chartOptions = angular.merge({
        valueFormatter: function(value) {
          return Math.floor(value / 1024 / 1024).toFixed(0) + ' MB';
        }
      }, sparkLineOptionBase);

    }])
;