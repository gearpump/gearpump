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
        network.on('resize', function(args) {
          network.fit();
        });
        scope.$watch('data', function (data) {
          if (data) {
            network.setOptions({physics: true});
            network.setData(data);
            network.once('stabilizationIterationsDone', function() {
              network.setOptions({physics: false});
            });
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
