/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .directive('visNetwork', [function() {
    'use strict';

    function handleOnContextEvent(network, callback) {
      var rightClickHandler = function(args) {
        function handleCallback(data) {
          // always disable the default context menu "Save image as, Copy Image..."
          vis.util.preventDefault(args.event);

          callback(angular.merge(data, {pointer: args.pointer}));
        }

        var selection = network.getNodeAt(args.pointer.DOM);
        if (angular.isDefined(selection)) {
          handleCallback({node: selection});
        } else {
          selection = network.getEdgeAt(args.pointer.DOM);
          if (angular.isDefined(selection)) {
            handleCallback({edge: selection});
          }
        }
      };

      network.on('oncontext', rightClickHandler);

      // 'load' is a touch-device only event, which is equivalent to right-click 'oncontext' event on PC.
      network.on('hold', rightClickHandler);
    }

    function overrideHoverNodeCallback(network, callback) {
      return function(args) {
        var nodeId = parseInt(args.node);
        var position = network.getBoundingBox(nodeId);
        position = network.canvasToDOM({x: position.left, y: position.top});
        callback({
          node: nodeId,
          position: position
        });
        vis.util.preventDefault(args.event);
      };
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
            case 'onClick':
              network.on('click', callback);
              break;
            case 'onDoubleClick':
              network.on('doubleClick', callback);
              break;
            case 'onSelectNode':
              network.on('selectNode', callback);
              break;
            case 'onDeselectNode':
              network.on('deselectNode', callback);
              break;
            case 'onHoverNode':
              network.on('hoverNode', overrideHoverNodeCallback(network, callback));
              break;
            case 'onBlurNode':
              network.on('blurNode', overrideHoverNodeCallback(network, callback));
              break;
            case 'onContext':
              handleOnContextEvent(network, callback);
              break;
            case 'onDeletePressed':
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