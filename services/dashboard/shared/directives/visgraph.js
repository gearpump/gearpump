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
        scope.$on('$destroy', function() {
          network.destroy();
        });

        angular.forEach(scope.events, function(callback, name) {
          if (['doubleClick', 'click'].indexOf(name) !== -1) {
            network.on(name, callback);
          }
        });

        if (scope.events.hasOwnProperty('oncontext')) {
          var contextCallback = scope.events['oncontext'];
          network.on('oncontext', function(args) {
            function handleOnContext(data) {
              if (contextCallback) {
                contextCallback(angular.merge(data, {pointer: args.pointer}));
              }
              vis.util.preventDefault(args.event);
            }
            var selection = network.getNodeAt(args.pointer.DOM);
            if (angular.isDefined(selection)) {
              handleOnContext({node: selection});
            } else {
              selection = network.getEdgeAt(args.pointer.DOM);
              if (angular.isDefined(selection)) {
                handleOnContext({edge: selection});
              }
            }
          });
        }

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
