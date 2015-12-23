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

  .controller('StreamingAppMetricsCtrl', ['$scope', '$sortableTableBuilder', 'models', 'conf',
    function($scope, $stb, models, conf) {
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

      $scope.$watch('names.selected', function(val) {
        if (angular.isFunction(stopWatchingFn)) {
          stopWatchingFn();
        }

        var watchClassPieces = metricClassLookup[val].split('.');
        var metricClassName = watchClassPieces[3];
        $scope.metricsGroup = watchClassPieces[2];

        stopWatchingFn = $scope.$watch('dag.metricsTime', function() {
          var metrics = _.mapValues($scope.dag.metrics, function(metricsGroups) {
            return metricsGroups[metricClassName];
          });

          if (_.keys(metrics).length) {
            var tableObj = $scope.metricsGroup === 'meter' ?
              $scope.meterMetricsTable : $scope.histogramMetricsTable;
            updateMetricsTable(tableObj, metrics);
          }
        });
      });

      $scope.meterMetricsTable = {
        cols: [
          $stb.indicator().key('active').canSort().styleClass('td-no-padding').done(),
          $stb.link('Processor').key(['processorId', 'taskPath']).canSort('processorId').sortDefault().styleClass('col-sm-1 text-nowrap').done(),
          $stb.number('Tasks').key('taskNum').canSort().styleClass('col-sm-1').done(),
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
          $stb.indicator().key('active').canSort().styleClass('td-no-padding').done(),
          $stb.link('Processor').key(['processorId', 'taskPath']).canSort('processorId').sortDefault().styleClass('col-sm-1 text-nowrap').done(),
          $stb.number('Tasks').key('taskNum').canSort().styleClass('col-sm-1').done(),
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
          _.map(metrics, function(metric, processorId) {
            var processor = $scope.dag.processors[processorId];
            var processorUrl = processor.active ?
              '#/apps/streamingapp/' + $scope.app.appId + '/processor/' + processorId : '';
            return angular.merge({
              active: {
                tooltip: processor.active ? '' : 'Not in use',
                condition: processor.active ? 'good' : '',
                shape: 'stripe'
              },
              processorId: processorId,
              taskPath: {
                text: 'Processor ' + processorId,
                href: processorUrl
              },
              taskNum: processor.parallelism,
              taskClass: processor.taskClass
            }, metric);
          })
        );
      }
    }])
;