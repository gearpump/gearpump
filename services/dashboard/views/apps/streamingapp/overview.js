/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .config(['$stateProvider',
    function($stateProvider) {
      'use strict';

      $stateProvider
        .state('streamingapp.overview', {
          url: '', /* default page */
          templateUrl: 'views/apps/streamingapp/overview.html',
          controller: 'StreamingAppOverviewCtrl'
        });
    }])

  .controller('StreamingAppOverviewCtrl', ['$scope', '$propertyTableBuilder',
    function($scope, $ptb) {
      'use strict';

      $scope.appSummary = [
        $ptb.text('ID').done(),
        $ptb.datetime('Start Time').done(),
        $ptb.text('User').done(),
        $ptb.text('Actor Path').done(),
        $ptb.button('Quick Links').done()
      ];

      function updateSummaryTable(app) {
        $ptb.$update($scope.appSummary, [
          app.appId,
          app.startTime,
          app.user,
          app.actorPath,
          [
            {href: app.configLink, text: 'Config', class: 'btn-xs'},
            {tooltip: app.homeDirectory, text: 'Home Dir.', class: 'btn-xs'},
            {tooltip: app.logFile, text: 'Log Dir.', class: 'btn-xs'}
          ]
        ]);
      }

      $scope.$watch('app', function(app) {
        updateSummaryTable(app);
      });
    }])
;