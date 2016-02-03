/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .config(['$stateProvider',
    function($stateProvider) {
      'use strict';

      $stateProvider
        .state('cluster.workers', {
          url: '/workers',
          // todo: listview and gridview are nested controllers
          templateUrl: 'views/cluster/workers/workers_listview.html',
          controller: 'WorkersListViewCtrl',
          resolve: {
            workers0: ['models', function(models) {
              return models.$get.workers();
            }]
          }
        });
    }])

  .controller('WorkersListViewCtrl', ['$scope', '$sortableTableBuilder', 'workers0',
    function($scope, $stb, workers0) {
      'use strict';

      $scope.workersTable = {
        cols: [
          // group 1/3 (4-col)
          $stb.indicator().key('state').canSort('state.condition+"_"+aliveFor').styleClass('td-no-padding').done(),
          $stb.link('ID').key('id').canSort().sortDefaultDescent().styleClass('col-md-1').done(),
          $stb.text('Address').key('akkaAddr').canSort().styleClass('col-md-1').done(),
          $stb.text('JVM Info').key('jvm')
            .help('Format: PID@hostname')
            .styleClass('col-md-2 hidden-xs').done(),
          // group 2/3 (5-col)
          $stb.number('Executors').key('executors').canSort().styleClass('col-md-1 hidden-xs').done(),
          $stb.progressbar('Slots Usage').key('slots').sortBy('slots.usage').styleClass('col-md-1').done(),
          $stb.duration('Uptime').key('aliveFor').canSort().styleClass('col-md-3 hidden-sm hidden-xs').done(),
          // group 3/3 (3-col)
          $stb.button('Quick Links').key(['detail', 'conf']).styleClass('col-md-3').done()
        ],
        rows: null
      };

      function updateTable(workers) {
        $scope.workersTable.rows = $stb.$update($scope.workersTable.rows,
          _.map(workers, function(worker) {
            return {
              id: {href: worker.pageUrl, text: worker.workerId},
              state: {tooltip: worker.state, condition: worker.isRunning ? 'good' : 'concern', shape: 'stripe'},
              akkaAddr: worker.akkaAddr,
              jvm: worker.jvmName,
              aliveFor: worker.aliveFor,
              slots: {current: worker.slots.used, max: worker.slots.total, usage: worker.slots.usage},
              executors: worker.executors.length || 0,
              detail: {href: worker.pageUrl, text: 'Details', class: 'btn-xs btn-primary'},
              conf: {href: worker.configLink, target: '_blank', text: 'Config', class: 'btn-xs'}
            };
          }));
      }

      updateTable(workers0.$data());
      workers0.$subscribe($scope, function(data) {
        updateTable(data);
      });
    }])
;