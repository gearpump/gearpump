/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .directive('visNetwork', [function() {
    'use strict';

    function overrideOnContextEvent(network, callback) {
      network.on('oncontext', function(args) {
        function handleOnContext(data) {
          if (callback) {
            callback(angular.merge(data, {pointer: args.pointer}));
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

    function overrideHoverNodeEvent(network, callback) {
      network.on('hoverNode', function(args) {
          if (callback) {
            var nodeId = parseInt(args.node);
            var radius = network.findNode(nodeId)[0].options.size;
            var position = network.getPositions([nodeId])[nodeId];
            position = network.canvasToDOM(position);
            callback({
              node: nodeId,
              radius: Math.ceil(radius),
              position: position
            });
          }
          vis.util.preventDefault(args.event);
        }
      );
    }

    function handleDeleteKeyPressed(network, callback) {
      var keys = vis.keycharm({
        container: network.dom
      });
      keys.bind('delete', function(event) {
        var selection = network.getSelection();
        callback(selection, event);
      });
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
          switch (name) {
            case 'click':
            case 'doubleClick':
              network.on(name, callback);
              break;
            case 'oncontext':
              overrideOnContextEvent(network, callback);
              break;
            case 'hoverNode':
              overrideHoverNodeEvent(network, callback);
              break;
            case 'ondeletepressed':
              handleDeleteKeyPressed(network, callback);
              break;
          }
        });

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