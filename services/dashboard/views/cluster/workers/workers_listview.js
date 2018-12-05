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
        .state('cluster.workers', {
          url: '/workers',
          // todo: listview and gridview are nested controllers
          templateUrl: 'views/cluster/workers/workers_listview.html',
          controller: 'WorkersListViewCtrl',
          resolve: {
            workers0: ['models', function (models) {
              return models.$get.workers();
            }],
            supervisor0: ['models', function (models) {
              return models.$get.supervisor();
            }]
          }
        });
    }])

  .controller('WorkersListViewCtrl', ['$scope', '$modal', '$sortableTableBuilder', '$dialogs',
    'restapi', 'workers0', 'supervisor0',
    function ($scope, $modal, $stb, $dialogs, restapi, workers0, supervisor0) {
      'use strict';

      $scope.isSupervisor = (supervisor0.path || '').length > 0;
      $scope.workersTable = {
        cols: [
          // group 1/3 (4-col)
          $stb.indicator().key('state').canSort('state.condition+"_"+aliveFor').styleClass('td-no-padding').done(),
          $stb.link('ID').key('id').canSort().sortDefaultDescent().done(),
          $stb.text('JVM Info').key('jvm').styleClass('col-md-1').done(),
          $stb.text('Address').key('akkaAddr').canSort().styleClass('col-md-3 hidden-xs').done(),
          // group 2/3 (5-col)
          $stb.number('Executors').key('executors').canSort().styleClass('col-md-1 hidden-xs').done(),
          $stb.progressbar('Slots Usage').key('slots').sortBy('slots.usage').styleClass('col-md-1').done(),
          $stb.duration('Uptime').key('aliveFor').canSort().styleClass('col-md-3 hidden-sm hidden-xs').done(),
          // group 3/3 (3-col)
          $stb.button('Quick Links').key(['detail', 'conf', 'kill']).styleClass('col-md-3').done()
        ],
        rows: null
      };

      function updateTable(workers) {
        $scope.workersTable.rows = $stb.$update($scope.workersTable.rows,
          _.map(workers, function (worker) {
            return {
              id: {href: worker.pageUrl, text: worker.workerId},
              state: {
                tooltip: worker.state,
                condition: worker.isRunning ? 'good' : 'concern',
                shape: 'stripe'
              },
              akkaAddr: worker.akkaAddr,
              jvm: worker.jvmName,
              aliveFor: worker.aliveFor,
              slots: {
                current: worker.slots.used,
                max: worker.slots.total,
                usage: worker.slots.usage
              },
              executors: worker.executors.length || 0,
              detail: {href: worker.pageUrl, text: 'Details', class: 'btn-xs btn-primary'},
              conf: {href: worker.configLink, target: '_blank', text: 'Config', class: 'btn-xs'},
              kill: {
                text: 'Kill', class: 'btn-xs',
                disabled: !$scope.isSupervisor,
                click: function () {
                  $dialogs.confirm('Are you sure to kill this worker?', function () {
                    restapi.removeWorker(worker.workerId,
                      function handleResponse(response) {
                        if (response.success) {
                          window.location.reload();
                        } else {
                          $dialogs.notice(response.error, 'Something went wrong');
                        }
                      },
                      function handleException(ex) {
                        $dialogs.notice(ex, 'Something went wrong');
                      }
                    );
                  });
                }
              },
            };
          }));
      }

      updateTable(workers0.$data());
      workers0.$subscribe($scope, function (data) {
        updateTable(data);
      });

      var addWorkerDialog = $modal({
        templateUrl: 'views/cluster/workers/add_worker.html',
        controller: 'AddWorkerCtrl',
        backdrop: 'static',
        keyboard: true,
        show: false
      });

      $scope.showAddWorkerDialog = function () {
        addWorkerDialog.$promise.then(addWorkerDialog.show);
      };
    }])
;