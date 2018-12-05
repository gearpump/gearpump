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
        .state('apps', {
          url: '/apps',
          templateUrl: 'views/apps/apps.html',
          controller: 'AppsCtrl',
          resolve: {
            apps0: ['models', function (models) {
              return models.$get.apps();
            }]
          }
        });
    }])

  .controller('AppsCtrl', ['$scope', '$modal', '$state', '$sortableTableBuilder', '$dialogs', 'apps0',
    function ($scope, $modal, $state, $stb, $dialogs, apps0) {
      'use strict';

      var submitWindow = $modal({
        templateUrl: 'views/apps/submit/submit.html',
        controller: 'AppSubmitCtrl',
        backdrop: 'static',
        keyboard: true,
        show: false
      });

      $scope.openSubmitGearAppDialog = function () {
        submitWindow.$scope.isStormApp = false;
        submitWindow.$promise.then(submitWindow.show);
      };

      $scope.openSubmitStormAppDialog = function () {
        submitWindow.$scope.isStormApp = true;
        submitWindow.$promise.then(submitWindow.show);
      };

      $scope.composeMenuOptions = [{
        text: '<i class="glyphicon glyphicon-none"></i> <b>Submit Gearpump Application...</b>',
        click: $scope.openSubmitGearAppDialog
      }, {
        text: '<i class="glyphicon glyphicon-none"></i> Submit Storm Application...',
        click: $scope.openSubmitStormAppDialog
      }, {
        text: '<i class="glyphicon glyphicon-pencil"></i> Compose DAG',
        href: $state.href('compose_app')
      }];

      $scope.appsTable = {
        cols: [
          // group 1/3 (4-col)
          $stb.indicator().key('state').canSort('state.condition+"_"+submissionTime').styleClass('td-no-padding').done(),
          $stb.link('ID').key('id').canSort().done(),
          $stb.link('Name').key('name').canSort('name.text').styleClass('col-md-1').done(),
          $stb.text('Address').key('akkaAddr').canSort().styleClass('col-md-3 hidden-sm hidden-xs').done(),
          // group 2/3 (5-col)
          $stb.datetime('Submission Time').key('submissionTime').canSort().sortDefaultDescent().styleClass('col-md-1 hidden-sm hidden-xs').done(),
          $stb.datetime('Start Time').key('startTime').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
          $stb.datetime('Stop Time').key('stopTime').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
          $stb.text('User').key('user').canSort().styleClass('col-md-2').done(),
          // group 3/3 (4-col)
          $stb.text('Status').key('status').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
          $stb.button('Actions').key(['view', 'config', 'kill', 'restart']).styleClass('col-md-3').done()
        ],
        rows: null
      };

      function updateTable(apps) {
        $scope.appsTable.rows = $stb.$update($scope.appsTable.rows,
          _.map(apps, function (app) {
            var pageUrl = app.isRunning ? app.pageUrl : '';
            return {
              id: {href: pageUrl, text: app.appId},
              name: {href: pageUrl, text: app.appName},
              state: {tooltip: app.status, condition: app.isRunning ? 'good' : '', shape: 'stripe'},
              akkaAddr: app.akkaAddr,
              user: app.user,
              submissionTime: app.submissionTime,
              startTime: app.startTime,
              stopTime: app.finishTime || '-',
              status: app.status,
              view: {
                href: app.pageUrl,
                text: 'Details',
                class: 'btn-xs btn-primary',
                disabled: !app.isRunning
              },
              config: {href: app.configLink, target: '_blank', text: 'Config', class: 'btn-xs'},
              kill: {
                text: 'Kill', class: 'btn-xs', disabled: app.isDead,
                click: function () {
                  $dialogs.confirm('Are you sure to kill this application?', function () {
                    app.terminate();
                  });
                }
              },
              restart: {
                text: 'Restart', class: 'btn-xs', disabled: !app.isRunning,
                click: function () {
                  $dialogs.confirm('Are you sure to restart this application?', function () {
                    app.restart();
                  });
                }
              }
            };
          }));
      }

      updateTable(apps0.$data());
      apps0.$subscribe($scope, function (apps) {
        updateTable(apps);
      });
    }])
;
