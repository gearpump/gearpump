/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .directive('appComputeResourceTable', function() {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'views/apps/app/executors_table.html',
      replace: false /* true will got an error */,
      controller: ['$scope', '$sortableTableBuilder',
        function($scope, $stb) {
          $scope.table = {
            cols: [
              $stb.indicator().key('status').canSort().styleClass('td-no-padding').done(),
              $stb.link('Name').key('id').canSort().sortDefault().styleClass('col-md-4').done(),
              $stb.link('Worker ID').key('worker').canSort().styleClass('col-md-8').done()
            ],
            rows: null
          };

          function updateTable(app) {
            $scope.table.rows = _.map(app.executors, function(executor) {
              return {
                status: {
                  tooltip: executor.status,
                  condition: executor.isRunning ? 'good' : '',
                  shape: 'stripe'
                },
                id: {href: executor.pageUrl, text: 'Executor ' + executor.executorId},
                worker: {href: executor.workerPageUrl, text: executor.workerId}
              };
            });
          }

          $scope.$watch('app', function(app) {
            updateTable(app);
          });
        }]
    };
  })
;