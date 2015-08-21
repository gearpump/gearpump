/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .directive('header', function() {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'views/landing/header.html',
      replace: true,
      scope: {},
      controller: ['$scope', 'restapi', function($scope, restapi) {
        $scope.version = 'beta';
        restapi.repeatHealthCheck($scope, function(version) {
          $scope.version = version;
        });
      }]
    };
  })
;