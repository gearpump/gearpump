/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('directive.visdag', [])
  .directive('visdag', function () {
    return {
      restrict: 'EA',
      transclude: false,
      scope: {
        data: '=',
        options: '='
      },
      link: function (scope, element, attr) {
        var network = new vis.Network(element[0], scope.data, scope.options);
        scope.$watch('data', function () {
          if (scope.data) {
            network.setData(scope.data);
            network.freezeSimulation(true);
          }
        });
        scope.$watchCollection('options', function (options) {
          network.setOptions(options);
        });
      }
    };
  })
;
