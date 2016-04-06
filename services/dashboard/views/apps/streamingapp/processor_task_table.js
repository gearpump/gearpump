/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .controller('StreamingAppProcessorTaskTableCtrl', ['$scope', '$sortableTableBuilder',
    function ($scope, $stb) {
      'use strict';

      $scope.$watch('taskMetrics', function (metrics) {
        var tableObj = $scope.metricType === 'meter' ?
          $scope.meterMetricsTable : $scope.histogramMetricsTable;
        updateMetricsTable(tableObj, metrics);
      });

      $scope.meterMetricsTable = {
        cols: [
          $stb.text('Task').key('task').canSort('id').sortDefault().styleClass('col-sm-1 text-nowrap').done(),
          // right
          $stb.number('Total Messages').key('count').canSort().unit('msg').styleClass('col-md-4 col-sm-3').done(),
          $stb.number('Mean Rate').key('meanRate').canSort().unit('msg/s').styleClass('col-md-1 col-sm-3').done(),
          $stb.number('MA 1m').key('movingAverage1m').canSort().unit('msg/s')
            .help('1-Minute Moving Average').styleClass('col-md-1 col-sm-3').done()
        ],
        rows: null
      };

      $scope.histogramMetricsTable = {
        cols: [
          $stb.text('Task').key('task').canSort('id').sortDefault().styleClass('col-sm-1 text-nowrap').done(),
          $stb.number2('Std. Dev.').key('stddev').canSort().unit('ms').styleClass('col-sm-1').done(),
          $stb.number2('Mean').key('mean').canSort().unit('ms').styleClass('col-sm-1').done(),
          $stb.number2('Median').key('median').canSort().unit('ms').styleClass('col-sm-1').done(),
          $stb.number2('p95%').key('p95').canSort().unit('ms')
            .help('The 95th percentage').styleClass('col-sm-1 hidden-xs').done(),
          $stb.number2('p99%').key('p99').canSort().unit('ms')
            .help('The 99th percentage').styleClass('col-md-1 hidden-sm hidden-xs').done(),
          $stb.number2('p99.9%').key('p999').canSort().unit('ms')
            .help('The 99.9th percentage').styleClass('col-md-1 hidden-sm hidden-xs').done()
        ],
        rows: null
      };

      function updateMetricsTable(table, metrics) {
        table.rows = $stb.$update(table.rows,
          _.map(metrics, function (metric, taskId) {
            return angular.extend(metric, {
              task: 'T' + taskId,
              id: Number(taskId)
            });
          })
        );
      }
    }])
;