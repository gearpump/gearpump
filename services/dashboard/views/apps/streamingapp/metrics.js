/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .config(['$stateProvider',
    function($stateProvider) {
      'use strict';

      $stateProvider
        .state('streamingapp.metrics', {
          url: '/metrics',
          templateUrl: 'views/apps/streamingapp/metrics.html',
          controller: 'StreamingAppMetricsCtrl'
        });
    }])

  .controller('StreamingAppMetricsCtrl', ['$scope', '$sortableTableBuilder',
    function($scope, $stb) {
      'use strict';

      var lookup = {
        'Message Receive Throughput': 'dag.metrics.meter.receiveThroughput',
        'Message Send Throughput': 'dag.metrics.meter.sendThroughput',
        'Average Message Processing Time': 'dag.metrics.histogram.processTime',
        'Average Message Receive Latency': 'dag.metrics.histogram.receiveLatency'
      };
      $scope.names = {available: Object.keys(lookup)};
      $scope.names.selected = $scope.names.available[0];
      $scope.types = function(name) {
        var clazz = lookup[name];
        return clazz.split('.')[2];
      };

      var stopWatchingFn = null;
      $scope.$watch('names.selected', function(val) {
        if (stopWatchingFn) {
          stopWatchingFn();
        }
        var watchClass = lookup[val];
        stopWatchingFn = $scope.$watchCollection(watchClass, function(metrics) {
          $scope.metricsGroup = watchClass.split('.')[2];
          switch ($scope.metricsGroup) {
            case 'meter':
              updateMeterMetricsTable(_.values(metrics));
              break;
            case 'histogram':
              updateHistogramMetricsTable(_.values(metrics));
              break;
          }
        });
      });

      $scope.meterMetricsTable = {
        cols: [
          $stb.text('Path').key('taskPath').canSort().sortDefault().styleClass('col-xs-2').done(),
          $stb.text('Task Class').key('taskClass').canSort().styleClass('col-lg-3 hidden-md hidden-sm hidden-xs').done(),
          $stb.number('Message Count').key('count').canSort().unit('msg').styleClass('col-xs-2').done(),
          $stb.number('Mean Rate').key('mean').canSort().unit('msg/s').styleClass('col-xs-1').done(),
          $stb.number('MA 1m').key('ma1').canSort().unit('msg/s')
            .help('1-Minute Moving Average').styleClass('col-xs-1').done(),
          $stb.number('MA 5m').key('ma5').canSort().unit('msg/s')
            .help('5-Minute Moving Average').styleClass('col-xs-1').done(),
          $stb.number('MA 15m').key('ma15').canSort().unit('msg/s')
            .help('15-Minute Moving Average').styleClass('col-xs-1').done()
        ],
        rows: null
      };

      $scope.histogramMetricsTable = {
        cols: [
          $stb.text('Path').key('taskPath').canSort().sortDefault().styleClass('col-xs-2').done(),
          $stb.text('Task Class').key('taskClass').canSort().styleClass('col-lg-3 hidden-md hidden-sm hidden-xs').done(),
          $stb.number('Sampling Points').key('count').canSort().styleClass('col-xs-2').done(),
          $stb.number('Mean Rate').key('mean').canSort().unit('ms').styleClass('col-xs-1').done(),
          $stb.number('Std. Dev.').key('stddev').canSort().unit('ms').styleClass('col-xs-1').done(),
          $stb.number('Median').key('median').canSort().unit('ms').styleClass('col-xs-1').done(),
          $stb.number('p95%').key('p95').canSort().unit('ms')
            .help('The 95th percentage').styleClass('col-xs-1').done(),
          $stb.number('p99%').key('p99').canSort().unit('ms')
            .help('The 99th percentage').styleClass('col-xs-1').done(),
          $stb.number('p99.9%').key('p999').canSort().unit('ms')
            .help('The 99.9th percentage').styleClass('col-xs-1').done()
        ],
        rows: null
      };

      function buildMetricsTaskPath(metric) {
        return 'processor' + metric.processorId + '.task' + metric.taskId;
      }

      function updateMeterMetricsTable(metrics) {
        $scope.meterMetricsTable.rows = _.map(metrics, function(metric) {
          return {
            taskPath: buildMetricsTaskPath(metric),
            taskClass: metric.taskClass,
            count: metric.values.count,
            mean: metric.values.meanRate,
            ma1: metric.values.movingAverage1m,
            ma5: metric.values.movingAverage5m,
            ma15: metric.values.movingAverage15m
          };
        });
      }

      function updateHistogramMetricsTable(metrics) {
        $scope.histogramMetricsTable.rows = _.map(metrics, function(metric) {
          return {
            taskPath: buildMetricsTaskPath(metric),
            taskClass: metric.taskClass,
            count: metric.values.count,
            mean: metric.values.mean,
            stddev: metric.values.stddev,
            median: metric.values.median,
            p95: metric.values.p95,
            p99: metric.values.p99,
            p999: metric.values.p999
          };
        });
      }
    }])
;