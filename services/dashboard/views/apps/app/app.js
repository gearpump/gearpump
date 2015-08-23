/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .config(['$stateProvider',
    function($stateProvider) {
      'use strict';

      $stateProvider
        .state('app', {
          abstract: true,
          url: '/apps/app/:appId',
          templateUrl: 'views/apps/app/app.html',
          controller: 'AppCtrl',
          resolve: {
            app0: ['$stateParams', 'models', function($stateParams, models) {
              return models.$get.app($stateParams.appId);
            }]
          }
        });
    }])

/**
 * This controller is used to obtain app. All nested views will read status from here.
 */
  .controller('AppCtrl', ['$scope', 'app0',
    function($scope, app0) {
      'use strict';

      $scope.app = app0.$data();
      app0.$subscribe($scope, function(app) {
        $scope.app = app;
      });
    }])
;