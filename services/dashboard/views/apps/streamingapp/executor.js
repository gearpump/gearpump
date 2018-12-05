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
        .state('streamingapp.executor', {
          url: '/executor/:executorId',
          templateUrl: 'views/apps/streamingapp/executor.html',
          controller: 'StreamingAppExecutorCtrl',
          resolve: {
            executor0: ['$stateParams', 'models', function ($stateParams, models) {
              return models.$get.appExecutor($stateParams.appId, $stateParams.executorId);
            }]
          }
        });
    }])

  .controller('StreamingAppExecutorCtrl', ['$scope', '$propertyTableBuilder',
    'helper', 'restapi', 'models', 'executor0',
    function ($scope, $ptb, helper, restapi, models, executor0) {
      'use strict';

      $scope.executorName = executor0.id === -1 ?
        'AppMaster' : ('Executor ' + executor0.id);
      $scope.overviewTable = [
        $ptb.text('JVM Info').done(),
        $ptb.text('Actor Path').done(),
        $ptb.link('Worker').done(),
        $ptb.number('Task Count').done(),
        $ptb.button('Quick Links').values([
            {
              href: restapi.appExecutorConfigLink($scope.app.appId, executor0.id),
              target: '_blank',
              text: 'Config',
              class: 'btn-xs'
            },
            helper.withClickToCopy({text: 'Log Dir.', class: 'btn-xs'}, executor0.logFile)
          ]
        ).done()
      ];

      function updateOverviewTable(executor) {
        $ptb.$update($scope.overviewTable, [
          executor.jvmName,
          executor.actorPath,
          {href: executor.workerPageUrl, text: 'Worker ' + executor.workerId},
          executor.taskCount
          /* placeholder for quick links, but they do not need to be updated */
        ]);
      }

      $scope.metricsConfig = $scope.app.historyMetricsConfig;
      $scope.executor = executor0.$data();
      updateOverviewTable($scope.executor);
      executor0.$subscribe($scope, function (executor) {
        $scope.executor = executor;
        updateOverviewTable(executor);
      });

      // Delegate JvmMetrics directive to manage metrics
      $scope.queryMetricsFnRef = function (all) {
        return all ?
          models.$get.appExecutorHistMetrics($scope.app.appId, executor0.id) :
          models.$get.appExecutorMetrics($scope.app.appId, executor0.id,
            $scope.metricsConfig.retainRecentDataIntervalMs);
      };
    }])
;
