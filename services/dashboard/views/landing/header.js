/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .directive('header', function () {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'views/landing/header.html',
      replace: true,
      scope: {},
      controller: ['$scope', '$cookies', 'restapi', 'conf', function ($scope, $cookies, restapi, conf) {
        $scope.clusterMenuItems = [
          {text: 'Master', pathPatt: 'master', icon: 'fa fa-laptop'},
          {text: 'Workers', pathPatt: 'workers', icon: 'fa fa-server'}
        ];

        $scope.username = $cookies.get('username');
        $scope.userMenuItems = [
          {text: 'Sign Out', href: conf.loginUrl, icon: 'glyphicon glyphicon-off'},
          {isDivider: true},
          {text: 'Documents', href: '//gearpump.io', icon: 'fa fa-book'},
          {text: 'GitHub', href: '//github.com/gearpump/gearpump', icon: 'fa fa-github'}
        ];

        $scope.version = 'beta';
        restapi.serviceVersion(function (version) {
          $scope.version = version;
        });
      }]
    };
  })
;