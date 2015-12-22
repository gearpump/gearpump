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

  .controller('StreamingAppMetricsCtrl', ['$scope', '$interval', '$sortableTableBuilder', 'models', 'conf',
    function($scope, $interval, $stb, models, conf) {
      'use strict';

      var metricClassLookup = {
        'Message Send Throughput': 'dag.metrics.meter.sendThroughput',
        'Message Receive Throughput': 'dag.metrics.meter.receiveThroughput',
        'Average Message Processing Time': 'dag.metrics.histogram.processTime',
        'Average Message Receive Latency': 'dag.metrics.histogram.receiveLatency'
      };

      $scope.names = {available: _.keys(metricClassLookup)};
      $scope.names.selected = $scope.names.available[0];
      $scope.types = function(name) {
        var clazz = metricClassLookup[name];
        return clazz.split('.')[2];
      };

      var stopWatchingFn = null;
      var timeoutPromise = null;

      $scope.$on('destroy', function() {
        $interval.cancel(timeoutPromise);
      });

      $scope.$watch('names.selected', function(val) {
        $interval.cancel(timeoutPromise);
        if (angular.isFunction(stopWatchingFn)) {
          stopWatchingFn();
        }

        var watchClass = metricClassLookup[val];
        timeoutPromise = $interval(function() {
          queryTaskMetrics(watchClass);
        }, conf.restapiQueryInterval);
        queryTaskMetrics(watchClass);

        stopWatchingFn = $scope.$watchCollection('taskMetrics', function(metrics) {
          if (angular.isObject(metrics)) {
            $scope.metricsGroup = watchClass.split('.')[2];
            if ($scope.metricsGroup === 'meter') {
              updateMetricsTable($scope.meterMetricsTable, metrics);
            } else if ($scope.metricsGroup === 'histogram') {
              updateMetricsTable($scope.histogramMetricsTable, metrics);
            }
          }
        });
      });

      function queryTaskMetrics(watchClass) {
        models.$get.currentAppTaskMetrics($scope.app.appId, watchClass)
          .then(function(metrics) {
            $scope.taskMetrics = metrics.$data();
          });
      }

      $scope.meterMetricsTable = {
        cols: [
          $stb.link('Path').key('taskPath').canSort().sortDefault().styleClass('col-sm-2').done(),
          $stb.text('Task Class').key('taskClass').canSort().styleClass('col-md-4 hidden-sm hidden-xs').done(),
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
          $stb.text('Path').key('taskPath').canSort().sortDefault().styleClass('col-sm-2').done(),
          $stb.text('Task Class').key('taskClass').canSort().styleClass('col-lg-4 hidden-md hidden-sm hidden-xs').done(),
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
          _.map(metrics, function(metric, path) {
            var info = extractTaskInfo(path);
            return angular.merge({
              taskPath: {
                text: info.path,
                href: '#/apps/streamingapp/' + $scope.app.appId + '/processor/' + info.processorId
              },
              taskClass: info.clazz
            }, metric.values);
          })
        );
      }

      function extractTaskInfo(path) {
        var pieces = path.split('.').slice(1);
        var processorId = parseInt(pieces[0].replace(/[^0-9]/g, ''));
        return {
          path: pieces.join('.'),
          processorId: processorId,
          clazz: $scope.dag.processors[processorId].taskClass
        };
      }
    }])
;