/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .factory('$visNetworkStyle', function() {
    'use strict';

    var fontFace = 'lato,roboto,"helvetica neue","segoe ui",arial,helvetica,sans-serif';
    var maxNodeRadius = 16;

    function nodeColorSet(border, background) {
      var colorSet = {
        border: border,
        background: background
      };
      var result = colorSet;
      result.hover = colorSet;
      result.highlight = colorSet;
      return result;
    }

    var concernedNodeColor = nodeColorSet('rgb(138,1,12)', 'rgb(248,106,91)');
    var criticalPathNodeColor = nodeColorSet('rgb(220,131,5)', 'rgb(250,161,35)');

    var self = {
      newOptions: function(height) {
        return {
          autoResize: true, // The network will automatically detect size changes and redraw itself accordingly
          interaction: {
            hover: true
          },
          width: '100%',
          height: height,
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
      newHierarchicalLayoutOptions: function(flags) {
        var verticalMargin = 28;
        var levelDistance = 85;
        var chartMinHeight = 240;
        var height = Math.max(chartMinHeight,
            (maxNodeRadius * (flags.depth + 1) + levelDistance * flags.depth + verticalMargin * 2)) + 'px';
        return angular.merge(self.newOptions(height), {
          layout: {
            hierarchical: {
              sortMethod: 'hubsize',
              direction: 'UD',
              levelSeparation: levelDistance
            }
          }
        });
      },
      newData: function() {
        return {
          nodes: new vis.DataSet(),
          edges: new vis.DataSet()
        };
      },
      nodeRadiusRange: function() {
        return [3, 16];
      },
      nodeColor: {
        normal: {
          border: '#2B7CE9',
          background: '#D2E5FF'
        },
        concerned: concernedNodeColor,
        criticalPath: criticalPathNodeColor
      },
      edgeWidthRange: function() {
        return [1, 5];
      },
      edgeArrowSizeRange: function() {
        return [0.5, 0.1];
      },
      edgeOpacityRange: function() {
        return [0.4, 1];
      },
      edgeColor: {
        normal: {
          color: '#2B7CE9',
          hover: '#2B7CE9',
          highlight: '#2B7CE9'
        },
        inactive: {
          color: 'rgb(195,195,195)',
          hover: 'rgb(166,166,166)',
          highlight: 'rgb(166,166,166)'
        },
        criticalPath: {
          color: 'rgb(250,161,35)',
          hover: 'rgb(220,131,5)',
          highlight: 'rgb(220,131,5)'
        }
      },
      /** Return label of processor name */
      processorNameAsLabel: function(processor) {
        return '[' + processor.id + '] ' +
          (processor.description || _.last(processor.taskClass.split('.')));
      }
    };
    return self;
  })
;