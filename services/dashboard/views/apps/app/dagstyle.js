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
        var verticalMargin = 28;
        var levelDistance = 85;
        var chartMinHeight = 220;
        return {
          autoResize: true, // The network will automatically detect size changes and redraw itself accordingly
          interaction: {
            hover: true
          },
          width: '100%',
          height: Math.max(chartMinHeight,
            (maxNodeRadius * (flags.depth + 1) + levelDistance * flags.depth + verticalMargin * 2)) + 'px',
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
      nodeColor: function (concern) {
        var colorSet = concern ? {
          border: 'rgb(138,1,12)',
          background: 'rgb(248,106,91)'
        } : {
          border: '#2B7CE9',
          background: '#D2E5FF'
        };
        var result = colorSet;
        if (concern) {
          result.hover = colorSet;
          result.highlight = colorSet;
        }
        return result;
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
        return alive ? {
          color: '#2B7CE9',
          hover: '#2B7CE9',
          highlight: '#2B7CE9'
        } : {
          color: 'rgb(195,195,195)',
          hover: 'rgb(166,166,166)',
          highlight: 'rgb(166,166,166)'
        };
      }
    };
  })
;
