/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.apps.appmaster')

  .factory('dagStyle', function () {
    var fontFace = "lato,'helvetica neue','segoe ui',arial,helvetica,sans-serif";
    var maxNodeRadius = 16;
    return {
      newOptions: function (flags) {
        var verticalMargin = 30;
        var levelDistance = 85;
        return {
          autoResize: true, // The network will automatically detect size changes and redraw itself accordingly
          interaction: {
            hover: true
          },
          width: '100%',
          height: (maxNodeRadius * (flags.depth + 1) + levelDistance * flags.depth + verticalMargin * 2) + 'px',
          layout: {
            hierarchical: {
              sortMethod: 'hubsize',
              direction: 'UD',
              levelSeparation: levelDistance
            }
          },
          nodes: {
            shape: 'dot',
            font: {
              size: 13,
              face: fontFace,
              strokeColor: '#fff',
              strokeWidth: 5
            }
          },
          edges: {
            arrows: {
              to: true
            },
            font: {
              size: 11,
              face: fontFace,
              align: 'middle'
            },
            color: {
              opacity: 0.75
            },
            smooth: true
          }
        };
      },
      newData: function () {
        return {
          nodes: new vis.DataSet(),
          edges: new vis.DataSet()
        };
      },
      nodeRadiusRange: function () {
        return [2, 16];
      },
      edgeWidthRange: function () {
        return [1, 5];
      },
      edgeArrowSizeRange: function () {
        return [0.5, 0.1];
      },
      edgeOpacityRange: function () {
        return [0.4, 1];
      },
      edgeColorSet: function (alive) {
        if (!alive) {
          return {
            color: 'rgb(195,195,195)',
            hover: 'rgb(166,166,166)',
            highlight: 'rgb(166,166,166)'
          };
        }
        // use built-in color set for alive nodes
      }
    };
  })
;
