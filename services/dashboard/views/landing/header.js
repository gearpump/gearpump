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
        $scope.menu = [
          {text: 'Cluster', pathPatt: '/cluster', href: '#/cluster', icon: 'glyphicon glyphicon-th-large'},
          {text: 'Applications', pathPatt: '/apps', href: '#/apps', icon: 'glyphicon glyphicon-tasks'}
        ];

        $scope.links = [
          {text: 'Docs', href: '//gearpump.io', icon: 'fa fa-book'},
          {text: 'GitHub', href: '//github.com/gearpump/gearpump', icon: 'fa fa-github'}
        ];

        $scope.dropdownMenuOptions = ([].concat($scope.menu).concat($scope.links))
          .map(function(item) {
            return {
              text: '<i class="' + item.icon + '"></i> ' + item.text,
              href: item.href
            };
          });

        $scope.version = 'beta';
        restapi.serviceVersion(function(version) {
          $scope.version = version;
        });
      }]
    };
  })
;