/*
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
        var radius = network.findNode(nodeId)[0].options.size;
        var position = network.getPositions([nodeId])[nodeId];
        position = network.canvasToDOM(position);
        callback({
          node: nodeId,
          radius: Math.ceil(radius),
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
