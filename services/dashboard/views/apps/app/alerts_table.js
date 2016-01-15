/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .directive('alertsTable', function() {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'views/apps/app/alerts_table.html',
      replace: false /* true will got an error */,
      scope: {
        alerts: '=alertsBind'
      },
      controller: ['$scope', '$sortableTableBuilder',
        function($scope, $stb) {
          $scope.table = {
            cols: [
              $stb.indicator().key('severity').canSort().styleClass('td-no-padding').done(),
              $stb.datetime('Time').key('time').canSort().sortDefault().styleClass('col-xs-4').done(),
              $stb.text('Message').key('message').canSort().styleClass('col-xs-8').done()
            ],
            rows: null
          };

          var severityLookup = {
            error: {text: 'Error', condition: 'danger'},
            warning: {text: 'Warning', condition: 'concern'}
          };

          function updateTable(alerts) {
            $scope.table.rows = $stb.$update($scope.table.rows,
              _.map(alerts, function(alert) {
                var severity = severityLookup[alert.severity];
                return {
                  severity: {
                    tooltip: severity.text,
                    condition: severity.condition,
                    shape: 'stripe'
                  },
                  time: alert.time,
                  message: alert.message
                };
              }));
          }

          $scope.$watch('alerts', function(alerts) {
            updateTable(alerts);
          });
        }]
    };
  })
;