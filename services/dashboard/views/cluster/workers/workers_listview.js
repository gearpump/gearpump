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
          $stb.indicator().key('state').canSort('state.condition+"_"+aliveFor').styleClass('td-no-padding').done(),
          $stb.link('ID').key('id').canSort().styleClass('col-md-1').done(),
          $stb.text('Location').key('location').canSort().sortDefault().styleClass('col-md-3').done(),
          $stb.number('Executors').key('executors').canSort().styleClass('col-md-1').done(),
          $stb.progressbar('Slots Usage').key('slots').sortBy('slots.usage')
            .help('Slot is a minimal compute unit. The usage indicates the computation capacity.')
            .styleClass('col-md-1').done(),
          $stb.duration('Up Time').key('aliveFor').canSort().styleClass('col-md-3').done(),
          $stb.button('Quick Links').key(['detail', 'conf']).styleClass('col-md-3').done()
        ],
        rows: null
      };

      function updateTable(workers) {
        $scope.workersTable.rows = _.map(workers, function(worker) {
          return {
            id: {href: worker.pageUrl, text: worker.workerId},
            state: {tooltip: worker.state, condition: worker.isRunning ? 'good' : 'concern', shape: 'stripe'},
            location: worker.location,
            aliveFor: worker.aliveFor,
            slots: {current: worker.slots.used, max: worker.slots.total, usage: worker.slots.usage},
            executors: worker.executors.length || 0,
            detail: {href: worker.pageUrl, text: 'Details', class: 'btn-xs btn-primary'},
            conf: {href: worker.configLink, text: 'Config', class: 'btn-xs'}
          };
        });
      }

      updateTable(workers0.$data());
      workers0.$subscribe($scope, function(data) {
        updateTable(data);
      });
    }])
;