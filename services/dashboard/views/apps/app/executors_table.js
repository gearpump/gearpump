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

  .directive('executorTable', function () {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'views/apps/app/executors_table.html',
      replace: false /* true will got an error */,
      scope: {
        executors: '=executorsBind'
      },
      controller: ['$scope', '$sortableTableBuilder', 'i18n',
        function ($scope, $stb, i18n) {
          $scope.whatIsExecutor = i18n.terminology.appExecutor;
          $scope.table = {
            cols: [
              $stb.indicator().key('status').canSort().styleClass('td-no-padding').done(),
              $stb.link('Name').key('id').canSort().sortDefault().styleClass('col-xs-4').done(),
              $stb.link('Worker').key('worker').canSort().styleClass('col-xs-4').done(),
              $stb.number('Tasks').key('tasks').canSort().styleClass('col-xs-4').done()
            ],
            rows: null
          };

          function updateTable(executors) {
            $scope.table.rows = $stb.$update($scope.table.rows,
              _.map(executors, function (executor) {
                return {
                  status: {
                    tooltip: executor.status,
                    condition: executor.isRunning ? 'good' : '',
                    shape: 'stripe'
                  },
                  id: {
                    href: executor.pageUrl, text: executor.executorId === -1 ?
                      'AppMaster' : 'Executor ' + executor.executorId
                  },
                  worker: {href: executor.workerPageUrl, text: 'Worker ' + executor.workerId},
                  tasks: executor.taskCount || 0
                };
              }));
          }

          $scope.$watch('executors', function (executors) {
            updateTable(executors);
          });
        }]
    };
  })
;
