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
        .state('streamingapp.metrics', {
          url: '/metrics',
          templateUrl: 'views/apps/streamingapp/metrics_table.html',
          controller: 'StreamingAppMetricsCtrl'
        });
    }])

  .controller('StreamingAppMetricsCtrl', ['$scope', '$sortableTableBuilder',
    function ($scope, $stb) {
      'use strict';

      $scope.$watch('dag.metricsUpdateTime', function () {
        reloadMetricsAndUpdateMetricsTable();
      });

      $scope.$watch('metricName', function () {
        reloadMetricsAndUpdateMetricsTable();
      });

      function reloadMetricsAndUpdateMetricsTable() {
        if ($scope.metricName) {
          var metrics = $scope.dag.getMetricsByMetricName($scope.metricName);
          var tableObj = $scope.metricType === 'meter' ?
            $scope.meterMetricsTable : $scope.histogramMetricsTable;
          updateMetricsTable(tableObj, metrics);
        }
      }

      $scope.meterMetricsTable = {
        cols: [
          $stb.indicator().key('active').canSort().styleClass('td-no-padding').done(),
          $stb.link('Processor').key(['processorId', 'taskPath']).canSort('processorId').sortDefault().styleClass('col-sm-1 text-nowrap').done(),
          $stb.number('Tasks').key('taskNum').canSort().styleClass('col-sm-2').done(),
          $stb.text('Task Class').key('taskClass').canSort().styleClass('col-md-4 hidden-sm hidden-xs').done(),
          // right
          $stb.number('Total Messages').key('count').canSort().unit('msg').styleClass('col-md-4 col-sm-3').done(),
          $stb.number('Mean Rate').key('meanRate').canSort().unit('msg/s').styleClass('col-md-1 hidden-sm hidden-xs').done(),
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
          $stb.number2('Std. Dev.').key('stddev').canSort().unit('ms').styleClass('col-sm-5').done(),
          $stb.number2('Mean').key('mean').canSort().unit('ms').styleClass('col-sm-1').done(),
          $stb.number2('Median').key('median').canSort().unit('ms').styleClass('col-sm-1').done(),
          $stb.number2('p95%').key('p95').canSort().unit('ms')
            .help('The 95th percentage').styleClass('col-sm-1 hidden-xs').done(),
          $stb.number2('p99%').key('p99').canSort().unit('ms')
            .help('The 99th percentage').styleClass('col-sm-1 hidden-xs').done(),
          $stb.number2('p99.9%').key('p999').canSort().unit('ms')
            .help('The 99.9th percentage').styleClass('col-sm-1 hidden-xs').done()
        ],
        rows: null
      };

      function updateMetricsTable(table, metrics) {
        table.rows = $stb.$update(table.rows,
          _.map(metrics, function (metric, processorId) {
            var processor = $scope.dag.getProcessor(processorId);
            var processorUrl = processor.active ?
            '#/apps/streamingapp/' + $scope.app.appId + '/processor/' + processorId : '';
            return angular.merge({
              active: {
                tooltip: processor.active ? 'Active' : (processor.replaced ? 'Replaced by another processor' : 'Dead'),
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
