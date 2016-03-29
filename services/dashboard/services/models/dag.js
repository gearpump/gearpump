/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('io.gearpump.models')

  .service('Dag', function() {
    'use strict';

    /** This class represents a DAG. The topology is immutable after creation. */
    function Dag(processors, edges) {
      this.processors = processors; // {map} key is processor id, value is processor object
      this.edges = edges; // {map} key is edge id (e.g. 1_2), value is edge object
      this.sortedProcessorIds = this._getProcessorIdsByTopologicalOrdering(); // array of processor id
      this.predecessorIds = this._createProcessorIdAndPredecessorIdMap(); // {map} key is processor id, value is an array of processor id
      this.degrees = this._calculateDegrees(); // {map} key is processor id, value is object with indegree and outdegree
      this.paths = this._findAllPaths(); // array of paths from source to sink
    }

    Dag.prototype = {

      /** Indicate whether the topology of specified processors and edges is equal to the current topology. */
      isEqual: function(processors, edges) {
        return _.isEqual(this.processors, processors)
          && _.isEqual(this.edges, edges);
      },

      /** Return processor ids as an array. */
      getProcessorIds: function() {
        return this.sortedProcessorIds;
      },

      /** Return source processor ids as an array. */
      getSourceProcessorIds: function() {
        return _.filter(this.getProcessorIds(), function(processorId) {
          return this.degrees[processorId].indegree === 0;
        }, this);
      },

      /** Return sink processor ids as an array. */
      getSinkProcessorIds: function() {
        return _.filter(this.getProcessorIds(), function(processorId) {
          return this.degrees[processorId].outdegree === 0;
        }, this);
      },

      /** Return an array of all paths that can travel from source processor to sink processor. */
      _findAllPaths: function() {
        return _(this.getSourceProcessorIds()).map(function(processorId) {
          return this._findAllPathsFrom(processorId);
        }, this).flatten().value();
      },

      _findAllPathsFrom: function(processorId) {
        var successorIds = this._findSuccessorIds(processorId);
        if (successorIds.length === 0) {
          return [processorId];
        }
        return _(successorIds).map(function(successorId) {
          var successorPaths = this._findAllPathsFrom(successorId);
          return _.map(successorPaths, function(successorPath) {
            return [processorId].concat(successorPath);
          });
        }, this).flatten().value();
      },

      /** Return the number of processors on the longest path. */
      hierarchyDepth: function() {
        return _.max(_.map(this.processors, 'hierarchy'));
      },

      _getProcessorIdsByTopologicalOrdering: function() {
        return _(this.processors).sortBy('hierarchy').map('id').value();
      },

      _createProcessorIdAndPredecessorIdMap: function() {
        var result = {};
        _.forEach(this.getProcessorIds(), function(processorId) {
          result[processorId] = this._findPredecessorIds(processorId);
        }, this);
        return result;
      },

      _findPredecessorIds: function(processorId) {
        var result = [];
        _.forEach(this.edges, function(edge) {
          if (edge.to === processorId) {
            result.push(edge.from);
          }
        });
        return result;
      },

      _findSuccessorIds: function(processorId) {
        var result = [];
        _.forEach(this.edges, function(edge) {
          if (edge.from === processorId) {
            result.push(edge.to);
          }
        });
        return result;
      },

      _calculateDegrees: function() {
        var result = {};
        _.forEach(this.processors, function(_, key) {
          result[key] = {indegree: 0, outdegree: 0};
        });

        _.forEach(this.edges, function(edge) {
          result[edge.from].outdegree++;
          result[edge.to].indegree++;
        });
        return result;
      },

      /**
       * Return the latency of critical path and all matched paths.
       * Note that the latency is the sum of all processors on the path.
       */
      calculateCriticalPathAndLatency: function(metricsProvider, time) {
        // calculate independent processor latency
        var candidates = {};
        _.forEach(this.sortedProcessorIds, function(processorId) {
          candidates[processorId] = {
            latency: this._getProcessorLatency(processorId, metricsProvider, time),
            path: [processorId]
          };
        }, this);

        // iteratively update processor's latency (and path) by adding its maximal predecessor's latency
        _.forEach(this.sortedProcessorIds, function(processorId) {
          var predecessorIds = this.predecessorIds[processorId];
          if (predecessorIds.length > 0) {
            var maxLatencyPredecessor = _.max(_.map(predecessorIds, function(predecessorId) {
              return candidates[predecessorId];
            }), 'latency');
            var current = candidates[processorId];
            current.latency += maxLatencyPredecessor.latency;
            current.path = maxLatencyPredecessor.path.concat(current.path);
          }
        }, this);

        // find the critical path latency
        var criticalPathLatency = _.max(_.map(candidates, 'latency'));

        // find the critical paths
        var criticalPaths = _.map(_.pick(candidates, function(candidate) {
          return candidate.latency === criticalPathLatency;
        }), 'path');

        return {
          latency: criticalPathLatency,
          paths: criticalPaths
        };
      },

      _getProcessorLatency: function(processorId, metricsProvider, time) {
        if (this.processors[processorId].hierarchy === 0) {
          return 0; // the latency of source process is set to 0
        }
        return metricsProvider.getAverageMessageReceiveLatency([processorId], time) +
          metricsProvider.getAverageMessageProcessingTime([processorId], time)
      }

    };

    return Dag;
  })
;