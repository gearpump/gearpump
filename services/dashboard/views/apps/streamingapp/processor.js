/*
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
angular.module('dashboard')

  .config(['$stateProvider',
    function ($stateProvider) {
      'use strict';

      $stateProvider
        .state('streamingapp.processor', {
          url: '/processor/:processorId',
          templateUrl: 'views/apps/streamingapp/processor.html',
          controller: 'StreamingAppProcessorCtrl'
        });
    }])

  .controller('StreamingAppProcessorCtrl', ['$scope', '$interval', '$stateParams', '$propertyTableBuilder',
    'i18n', 'models', 'conf', 'helper',
    function ($scope, $interval, $stateParams, $ptb, i18n, models, conf, helper) {
      'use strict';

      $scope.whatIsProcessor = i18n.terminology.processor;
      $scope.processor = $scope.dag.getProcessor($stateParams.processorId);
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
        var degree = $scope.dag.getProcessorIndegreeAndOutdegree(processor.id);
        $ptb.$update($scope.processorInfoTable, [
          processor.taskClass,
          processor.parallelism,
          describeProcessorType(degree.indegree, degree.outdegree),
          processor.life.birth <= 0 ?
            'Start with the application' : processor.life.birth,
          processor.life.death === models.DAG_DEATH_UNSPECIFIED ?
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
      $scope.$on('$destroy', function () {
        $interval.cancel(promise);
      });
      $scope.$watch('metricName', function () {
        queryMetrics();
      });

      $scope.queryLimit = conf.restapiTaskLevelMetricsQueryLimit;
      $scope.shouldPaginateTasks = $scope.processor.parallelism > $scope.queryLimit;
      var requesting = false;

      function queryMetrics() {
        if (!requesting && $scope.metricName) {
          requesting = true;
          models.$get.appTaskLatestMetricValues(
            $scope.app.appId, $scope.processor.id, $scope.metricName, $scope.taskRange).then(function (metrics) {
              if (metrics.hasOwnProperty($scope.metricName)) {
                $scope.taskMetrics = metrics[$scope.metricName];
              }
              requesting = false;
            });
        }
      }

      promise = $interval(queryMetrics, conf.restapiQueryInterval);
      queryMetrics();

      $scope.whatIsTask = i18n.terminology.task;
      $scope.taskRange = {
        start: 0,
        stop: $scope.shouldPaginateTasks ?
        $scope.queryLimit - 1 : $scope.processor.parallelism - 1
      };

      $scope.$watch('taskRange', function (range) {
        if (range.hasOwnProperty('start')) {
          updateTaskSelection(range);
        }
      }, /*deep=*/true);

      function updateTaskSelection(range) {
        $scope.tasks = {
          selected: [],
          available: function () {
            var count = range.stop - range.start + 1;
            return _.times(count, function (i) {
              return 'T' + (i + range.start);
            });
          }()
        };
      }

      // For the bar chart control
      $scope.tasksBarChart = {
        options: {
          height: '110px',
          seriesNames: [''],
          barMinWidth: 4,
          barMinSpacing: 1,
          valueFormatter: function (value) {
            var unit = $scope.metricType === 'meter' ? 'msg/s' : 'ms';
            return helper.readableMetricValue(value) + ' ' + unit;
          },
          data: _.map($scope.tasks.available.length, function (taskName) {
            return {x: taskName, y: 0};
          })
        }
      };

      $scope.$watch('taskMetrics', function (metricsSelection) {
        if (angular.isObject(metricsSelection)) {
          updateBarChartData(metricsSelection);
        }
      });

      function updateBarChartData(metricsSelection) {
        if ($scope.tasks.available.length === 0 ||
          _.keys(metricsSelection).length !== $scope.tasks.available.length) {
          return;
        }

        var data = _.map($scope.tasks.available, function (taskName) {
          return {x: taskName};
        });
        var i = 0;
        var metricField = $scope.metricType === 'meter' ? 'movingAverage1m' : 'mean';
        _.forEach(metricsSelection, function (metric) {
          data[i].y = helper.metricRounded(metric[metricField]);
          i++;
        });

        var xLabelChanged = !_.isEqual(_.map($scope.tasksBarChart.options.data, 'x'), _.map(data, 'x'));
        if (xLabelChanged) {
          $scope.tasksBarChart.options.data = data;
        } else {
          $scope.tasksBarChart.data = data;
        }
      }
    }])
;
