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

      $scope.$watch('app', function(app) {
        $scope.appSummary = angular.merge({}, $scope.appSummary, [
          {value: app.appId},
          {value: app.startTime},
          {value: app.user},
          {value: app.actorPath},
          {values: [
            {href: app.configLink, text: 'Config', class: 'btn-xs'},
            {tooltip: app.homeDirectory, text: 'Home Dir.', class: 'btn-xs'},
            {tooltip: app.logFile, text: 'Log Dir.', class: 'btn-xs'}
          ]}
        ]);
      });
    }])
;
