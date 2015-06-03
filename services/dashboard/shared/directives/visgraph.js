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
        options: '=',
        events: '='
      },
      link: function (scope, elem) {
        var network = new vis.Network(elem[0], scope.data, scope.options);
        angular.forEach(scope.events, function(callback, name) {
          network.on(name, callback);
        });
        scope.$watch('data', function (data) {
          if (data) {
            network.once('stabilized', function() {
              network.setOptions({physics: false});
            });
            network.setData(data);
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
