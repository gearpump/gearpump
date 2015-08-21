/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .directive('visNetwork', [function() {
    'use strict';

    function overrideOnContextEvent(network, contextCallback) {
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

    function overrideHoverNodeEvent(network, hoverNodeCallback) {
      network.on('hoverNode', function(args) {
          if (hoverNodeCallback) {
            var nodeId = parseInt(args.node);
            var radius = network.findNode(nodeId)[0].options.size;
            var position = network.getPositions([nodeId])[nodeId];
            position = network.canvasToDOM(position);
            hoverNodeCallback({
              node: nodeId,
              radius: Math.ceil(radius),
              position: position
            });
          }
          vis.util.preventDefault(args.event);
        }
      );
    }

    return {
      restrict: 'E',
      scope: {
        data: '=',
        options: '=',
        events: '='
      },
      link: function(scope, elem) {
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
          overrideOnContextEvent(network, scope.events.oncontext);
        }

        if (scope.events.hasOwnProperty('hoverNode')) {
          overrideHoverNodeEvent(network, scope.events.hoverNode);
        }

        network.on('resize', function() {
          network.fit();
        });
        scope.$watch('data', function(data) {
          if (data) {
            network.setOptions({physics: true});
            network.setData(data);
            network.once('stabilizationIterationsDone', function() {
              network.setOptions({physics: false});
            });
          }
        });
        scope.$watchCollection('options', function(options) {
          if (options) {
            network.setOptions(options);
          }
        });
      }
    };
  }])
;