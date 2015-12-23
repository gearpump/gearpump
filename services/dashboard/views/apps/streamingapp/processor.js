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
          controller: 'StreamingAppProcessorCtrl',
          resolve: {
            metrics0: ['$stateParams', 'models', function($stateParams, models) {
              return models.$get.currentAppProcessorMetrics($stateParams.appId, $stateParams.processorId);
            }]
          }
        });
    }])

  .controller('StreamingAppProcessorCtrl', ['$scope', '$stateParams', '$propertyTableBuilder', 'metrics0',
    function($scope, $stateParams, $ptb, metrics0) {
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
          available: function() {
            return _.times(processor.parallelism, function(i) {
              return 'T' + i;
            });
          }()
        };

        // if only processor has only one task, select it by default. Corresponding charts will be shown automatically.
        if ($scope.tasks.available.length === 1) {
          $scope.tasks.selected = $scope.tasks.available;
        }
      }

      updateProcessorInfoTable($scope.processor);

      $scope.metrics = metrics0.$data();
      metrics0.$subscribe($scope, function(metrics) {
        $scope.metrics = metrics;
      });

      $scope.$watch('metrics', function(metrics) {
        if (angular.isObject(metrics)) {
          $scope.receiveSkewChart.data = updatedSkewData(metrics.receiveThroughput);
        }
      });

      var skewData = _.map($scope.tasks.available, function(taskName) {
        return {x: taskName};
      });

      function updatedSkewData(data) {
        angular.forEach(data, function(metric, taskId) {
          skewData[taskId].y = metric.values.meanRate;
        });
        return skewData;
      }

      $scope.receiveSkewChart = {
        options: {
          height: '110px',
          seriesNames: [''],
          barMinWidth: 10,
          barMinSpacing: 2,
          valueFormatter: function(value) {
            return Number(value).toFixed(0) + ' msg/s';
          },
          data: updatedSkewData(metrics0.receiveThroughput)
        }
      };
    }])
;