/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('directive.visgraph', [])

/** Directive of visjs's Network */
  // TODO: Operate network component
  .directive('visnetwork', [function () {
    return {
      restrict: 'EA',
      transclude: false,
      scope: {
        data: '=',
        options: '='
      },
      link: function (scope, elem) {
        var network = new vis.Network(elem[0], scope.data, scope.options);
        scope.$watch('data', function (data) {
          if (data) {
            network.setData(data);
            network.freezeSimulation(true);
          }
        });
        scope.$watchCollection('options', function (options) {
          if (options) {
            network.setOptions(options);
          }
        });
      }
    };
  }])
;
