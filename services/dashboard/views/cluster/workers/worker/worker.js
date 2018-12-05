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
      /**
       * todo: worker page uses full-width layout, but cluster.master and cluster.workers do not. So we use state
       * 'worker' instead of 'cluster.worker' for the time being, until a new layout is discussed.
       */
        .state('worker', {
          url: '/cluster/workers/worker/:workerId',
          templateUrl: 'views/cluster/workers/worker/worker.html',
          controller: 'WorkerCtrl',
          resolve: {
            worker0: ['$stateParams', 'models', function ($stateParams, models) {
              return models.$get.worker($stateParams.workerId);
            }],
            apps0: ['models', function (models) {
              return models.$get.apps();
            }]
          }
        });
    }])

  .controller('WorkerCtrl', ['$scope', '$propertyTableBuilder', '$sortableTableBuilder',
    'i18n', 'helper', 'models', 'locator', 'worker0', 'apps0',
    function ($scope, $ptb, $stb, i18n, helper, models, locator, worker0, apps0) {
      'use strict';

      $scope.whatIsWorker = i18n.terminology.worker;
      $scope.overviewTable = [
        $ptb.text('JVM Info').done(),
        $ptb.text('Actor Path').done(),
        $ptb.number('Slots Capacity').done(),
        $ptb.button('Quick Links').values([
          {href: worker0.configLink, target: '_blank', text: 'Config', class: 'btn-xs'},
          helper.withClickToCopy({text: 'Home Dir.', class: 'btn-xs'}, worker0.homeDirectory),
          helper.withClickToCopy({text: 'Log Dir.', class: 'btn-xs'}, worker0.logFile)
        ]).done()
      ];

      $scope.whatIsExecutor = i18n.terminology.workerExecutor;
      $scope.executorsTable = {
        cols: [
          $stb.indicator().key('status').canSort().styleClass('td-no-padding').done(),
          $stb.link('Application').key('id').canSort().styleClass('col-xs-4').done(),
          $stb.link('Executor').key('executors').canSort().styleClass('col-xs-4').done(),
          $stb.number('Slots').key('slots').canSort().sortDefaultDescent().styleClass('col-xs-4').done()
        ],
        rows: null
      };

      function updateOverviewTable(worker) {
        $ptb.$update($scope.overviewTable, [
          worker.jvmName,
          worker.actorPath,
          worker.slots.total
          /* placeholder for quick links, but they do not need to be updated */
        ]);
        $scope.uptimeCompact = helper.readableDuration(worker.aliveFor);
      }

      function updateExecutorsTable() {
        $scope.executorsTable.rows = $stb.$update($scope.executorsTable.rows,
          _.map($scope.worker.executors, function (executor) {
            if ($scope.apps.hasOwnProperty(executor.appId)) {
              var app = $scope.apps[executor.appId];
              var executorPageUrl = locator.executor(app.appId, app.type, executor.executorId);
              var executorPath = 'app' + executor.appId + '.' +
                (executor.executorId === -1 ?
                  'appmaster' : 'executor' + executor.executorId);
              return {
                status: {condition: 'good', shape: 'stripe'}, // always be good
                id: {href: app.pageUrl, text: app.appName},
                executors: {href: executorPageUrl, text: executorPath},
                slots: executor.slots
              };
            } else {
              console.warn({message: 'Unknown executor', executor: executor});
            }
          }));
      }

      $scope.metricsConfig = worker0.historyMetricsConfig;
      $scope.worker = worker0.$data();
      $scope.apps = apps0.$data();
      $scope.appsCount = Object.keys($scope.apps).length;

      updateOverviewTable($scope.worker);
      updateExecutorsTable();

      worker0.$subscribe($scope, function (worker) {
        updateWorkerDetails(worker);
      }, /*onerror=*/function () {
        // manually reset status fields on an error response
        var worker = angular.copy($scope.worker);
        worker.state = 'terminated';
        worker.isRunning = false;
        _.forEach(worker.executors, function (executor) {
          executor.status = 'terminated';
          executor.isRunning = false;
        });
        updateWorkerDetails(worker);
      });

      function updateWorkerDetails(worker) {
        $scope.worker = worker;
        updateOverviewTable(worker);
        updateExecutorsTable();
      }

      apps0.$subscribe($scope, function (apps) {
        $scope.apps = apps;
        updateExecutorsTable();
      });

      // Delegate JvmMetrics directive to manage metrics
      $scope.queryMetricsFnRef = function (all) {
        return all ?
          models.$get.workerHistMetrics(worker0.workerId) :
          models.$get.workerMetrics(worker0.workerId, $scope.metricsConfig.retainRecentDataIntervalMs);
      };
    }])
;