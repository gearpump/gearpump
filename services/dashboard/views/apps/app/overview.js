/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .config(['$stateProvider',
    function($stateProvider) {
      'use strict';

      $stateProvider
        .state('app.overview', {
          url: '', /* default page */
          templateUrl: 'views/apps/app/overview.html',
          controller: 'AppOverviewCtrl'
        });
    }])

  .controller('AppOverviewCtrl', ['$scope', 'helper', '$propertyTableBuilder', 'models',
    function($scope, helper, $ptb, models) {
      'use strict';

      $scope.appSummary = [
        $ptb.text('ID').done(),
        $ptb.text('Actor Path').done(),
        $ptb.datetime('Start Time').done(),
        $ptb.text('User').done(),
        $ptb.button('Quick Links').done()
      ];

      $scope.$watch('app', function(app) {
        $ptb.$update($scope.appSummary, [
          app.appId,
          app.actorPath,
          app.startTime,
          app.user,
          [
            {href: app.configLink, target: '_blank', text: 'Config', class: 'btn-xs'},
            helper.withClickToCopy({text: 'Home Dir.', class: 'btn-xs'}, app.homeDirectory),
            helper.withClickToCopy({text: 'Log Dir.', class: 'btn-xs'}, app.logFile)
          ]
        ]);
      });

      $scope.alerts = [];
      models.$get.appAlerts($scope.app.appId)
        .then(function(alerts0) {
          $scope.alerts = alerts0.$data();
          alerts0.$subscribe($scope, function(alerts) {
            $scope.alerts = alerts;
          });
        });
    }])
;