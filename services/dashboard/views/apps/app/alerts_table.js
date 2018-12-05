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

  .directive('alertsTable', function () {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'views/apps/app/alerts_table.html',
      replace: false /* true will got an error */,
      scope: {
        alerts: '=alertsBind'
      },
      controller: ['$scope', '$sortableTableBuilder',
        function ($scope, $stb) {
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
              _.map(alerts, function (alert) {
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

          $scope.$watch('alerts', function (alerts) {
            updateTable(alerts);
          });
        }]
    };
  })
;
