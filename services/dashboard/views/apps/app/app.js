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
            modelApp: ['$stateParams', 'models', function($stateParams, models) {
              return models.$get.app.detail($stateParams.appId);
            }]
          }
        });
    }])

/**
 * This controller is used to obtain app. All nested views will read status from here.
 */
  .controller('AppCtrl', ['$scope', 'modelApp',
    function($scope, modelApp) {
      'use strict';

      $scope.app = modelApp.$data();
      modelApp.$subscribe($scope, function(app) {
        $scope.app = app;
      });
    }])
;