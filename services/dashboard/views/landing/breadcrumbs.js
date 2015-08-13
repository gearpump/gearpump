/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .directive('breadcrumbs', function() {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'views/landing/breadcrumbs.html',
      replace: true,
      scope: {},
      controller: ['$scope', function($scope) {

        $scope.$on('$stateChangeSuccess', function() {
          $scope.breadcrumbs = buildBreadcrumbs();
        });

        function buildBreadcrumbs() {
          var paths = window.location.hash.split('/').splice(1);
          var breadcrumbs = [];
          for (var i = 0; i < paths.length; i++) {
            var label = paths[i];
            if (i + 1 < paths.length && !isNaN(paths[i + 1])) {
              label += ' ' + paths[i + 1];
              i++;
            }
            breadcrumbs.push({
              path: paths.slice(0, i + 1).join('/'),
              text: label
            });
          }

          // Quick and dirty way to replace breadcrumb text
          if (breadcrumbs.length > 0 && breadcrumbs[0].text === 'apps') {
            breadcrumbs[0].text = 'Applications';
            if (breadcrumbs.length > 1) {
              var appText = 'app ';
              var p = breadcrumbs[1].text.indexOf(appText);
              breadcrumbs[1].text = 'Application ' + breadcrumbs[1].text.substr(p + appText.length);
            }
          }
          return breadcrumbs;
        }
      }]
    };
  })
;