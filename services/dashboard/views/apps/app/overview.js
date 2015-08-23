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

  .controller('AppOverviewCtrl', ['$scope', '$propertyTableBuilder',
    function($scope, $ptb) {
      'use strict';

      $scope.appSummary = [
        $ptb.text('ID').done(),
        $ptb.datetime('Start Time').done(),
        $ptb.text('Actor Path').done(),
        $ptb.button('Quick Links').done()
      ];

      $scope.$watch('app', function(app) {
        $ptb.$update($scope.appSummary, [
          app.appId,
          app.startTime,
          app.actorPath,
          {href: app.configLink, text: 'Config', class: 'btn-xs'}
        ]);
      });
    }])
;