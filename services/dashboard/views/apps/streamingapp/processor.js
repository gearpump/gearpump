/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .config(['$stateProvider',
    function($stateProvider) {
      'use strict';

      $stateProvider
        .state('streamingapp.processor', {
          url: '/processor/:processorId',
          templateUrl: 'views/apps/streamingapp/processor.html',
          controller: 'StreamingAppProcessorCtrl'
        });
    }])

  .controller('StreamingAppProcessorCtrl', ['$scope', '$interval', '$stateParams', '$propertyTableBuilder',
    'models', 'conf', 'helper',
    function($scope, $interval, $stateParams, $ptb, models, conf, helper) {
      'use strict';

      $scope.processor = $scope.dag.processors[$stateParams.processorId];
      $scope.processorInfoTable = [
        $ptb.text('Task Class').done(),
        $ptb.number('Parallelism').done(),
        $ptb.text('Data Flow Type').done(),
        $ptb.datetime('Birth Time').done(),
        $ptb.datetime('Death Time').done()
      ];

      function describeProcessorType(inputs, outputs) {
        if (inputs === 0) {
          return outputs > 0 ? 'Data Source (%s outputs)'.replace('%s', outputs) : 'Orphan';
        } else if (outputs === 0) {
          return inputs > 0 ? 'Data Sink (%s inputs)'.replace('%s', inputs) : 'Orphan';
        }
        return 'General (%s inputs %s outputs)'.replace('%s', inputs).replace('%s', outputs);
      }

      function updateProcessorInfoTable(processor) {
        var connections = $scope.dag.calculateProcessorConnections(processor.id);
        $ptb.$update($scope.processorInfoTable, [
          processor.taskClass,
          processor.parallelism,
          describeProcessorType(connections.inputs, connections.outputs),
          processor.life.birth <= 0 ?
            'Start with the application' : processor.life.birth,
          processor.life.death === '9223372036854775807' /* Long.max */ ?
            'Not specified' : processor.life.death
        ]);

        $scope.tasks = {
          selected: [],
          available: []
        };
      }

      updateProcessorInfoTable($scope.processor);

      // query task metrics
      var promise;
      $scope.$on('$destroy', function() {
        $interval.cancel(promise);
      });
      $scope.$watch('metricClass', function() {
        queryMetrics();
      });

      $scope.queryLimit = conf.restapiTaskLevelMetricsQueryLimit;
      $scope.shouldPaginateTasks = $scope.processor.parallelism > $scope.queryLimit;
      var requesting = false;

      function queryMetrics() {
        if (!requesting && $scope.metricClass) {
          requesting = true;
          models.$get.appTaskLatestMetricValues(
            $scope.app.appId, $scope.processor.id, $scope.metricClass, $scope.taskRange).then(function(metrics) {
              if (metrics.hasOwnProperty($scope.metricClass)) {
                $scope.taskMetrics = metrics[$scope.metricClass];
              }
              requesting = false;
            });
        }
      }

      promise = $interval(queryMetrics, conf.restapiQueryInterval);
      queryMetrics();

      $scope.taskRange = $scope.shouldPaginateTasks ?
        {} : {start: 0, stop: $scope.processor.parallelism - 1};
      $scope.$watch('taskRange', function(range) {
        if (!range.hasOwnProperty('start')) {
          return;
        }
        $scope.tasks = {
          selected: [],
          available: function() {
            var count = range.stop - range.start + 1;
            return _.times(count, function(i) {
              return 'T' + (i + range.start);
            });
          }()
        };
      });

      // For the bar chart control (todo: dashing bar-chart should provide a way to rebuild chart data)
      $scope.tasksBarChart = null;
      $scope.$watch('taskMetrics', function(metrics) {
        var scopeIsReady = $scope.tasks.hasOwnProperty('available') && $scope.tasks.available.length;
        if (scopeIsReady && metrics) {
          updateBarChartData(metrics);
        }
      });

      function updateBarChartData(metrics) {
        if (!$scope.tasksBarChart) {
          $scope.tasksBarChart = {
            options: {
              height: '110px',
              seriesNames: [''],
              barMinWidth: 2,
              barMinSpacing: 2,
              valueFormatter: function(value) {
                var unit = $scope.metricType === 'meter' ? 'msg/s' : 'ms';
                return helper.metricValue(value) + ' ' + unit;
              },
              data: _.map($scope.tasks.available, function(taskName) {
                return {x: taskName};
              })
            }
          };
        }

        var data = $scope.tasksBarChart.options.data;
        if (data[0].x !== $scope.tasks.available[0]) {
          $scope.tasksBarChart = null;
          _.defer(updateBarChartData, data);
          return;
        }

        var i = 0;
        var metricField = $scope.metricType === 'meter' ? 'movingAverage1m' : 'mean';
        _.forEach(metrics, function(metric, taskId) {
          data[i].y = metric[metricField]
          ;
          i++;
        });
        $scope.tasksBarChart.data = angular.copy(data);
      }
    }])
;