/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .config(['$stateProvider',
    function($stateProvider) {
      'use strict';

      $stateProvider
        .state('apps', {
          url: '/apps',
          templateUrl: 'views/apps/apps.html',
          controller: 'AppsCtrl',
          resolve: {
            apps0: ['models', function(models) {
              return models.$get.apps();
            }]
          }
        });
    }])

  .controller('AppsCtrl', ['$scope', '$modal', '$state', '$sortableTableBuilder', 'apps0',
    function($scope, $modal, $state, $stb, apps0) {
      'use strict';

      var submitWindow = $modal({
        templateUrl: 'views/apps/submit/submit.html',
        controller: 'AppSubmitCtrl',
        backdrop: 'static',
        keyboard: false /* https://github.com/mgcrea/angular-strap/issues/1779 */,
        show: false
      });

      $scope.openSubmitDialog = function() {
        submitWindow.$promise.then(submitWindow.show);
      };

      $scope.composeMenuOptions = [{
        text: '<i class="glyphicon glyphicon-pencil"></i> Compose DAG',
        href: $state.href('compose_app')
      }, {
        text: '<i class="glyphicon glyphicon-none"></i> <b>Submit Application</b>',
        click: $scope.openSubmitDialog
      }];

      $scope.appsTable = {
        cols: [
          $stb.indicator().key('state').canSort('state.condition+"_"+submissionTime').styleClass('td-no-padding').done(),
          $stb.link('ID').key('id').canSort().done(),
          $stb.link('Name').key('name').canSort('name.text').styleClass('col-md-2').done(),
          $stb.datetime('Submission Time').key('submissionTime').canSort().sortDefaultDescent().styleClass('col-md-1').done(),
          $stb.datetime('Start Time').key('startTime').canSort().styleClass('col-md-1').done(),
          $stb.datetime('Stop Time').key('stopTime').canSort().styleClass('col-md-1').done(),
          $stb.text('User').key('user').canSort().styleClass('col-md-2').done(),
          $stb.text('Location').key('location').canSort()
            .help('The location where application master is running at')
            .styleClass('col-md-2 hidden-sm hidden-xs').done(),
          $stb.button('Actions').key(['view', 'kill', 'restart']).styleClass('col-md-3').done()
        ],
        rows: null
      };

      function updateTable(apps) {
        $scope.appsTable.rows = _.map(apps, function(app) {
          return {
            id: {href: app.pageUrl, text: app.appId},
            name: {href: app.pageUrl, text: app.appName},
            state: {tooltip: app.status, condition: app.isRunning ? 'good' : '', shape: 'stripe'},
            location: app.location,
            user: app.user,
            submissionTime: app.submissionTime,
            startTime: app.startTime,
            stopTime: app.finishTime || '-',
            view: {href: app.pageUrl, text: 'Details', class: 'btn-xs btn-primary', hide: !app.isRunning},
            kill: {
              text: 'Kill', class: 'btn-xs', hide: !app.isRunning,
              click: function() {
                app.terminate();
              }
            },
            restart: {
              text: 'Restart', class: 'btn-xs', hide: !app.isRunning,
              click: function() {
                app.restart();
              }
            }
          };
        });
      }

      updateTable(apps0.$data());
      apps0.$subscribe($scope, function(apps) {
        updateTable(apps);
      });
    }])
;