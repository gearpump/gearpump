/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

/** TODO: to be absorbed as scalajs */
  .service('StreamingDag', function() {
    'use strict';

    /** The constructor */
    function StreamingDag(clock, processors, levels, edges) {
      this.metrics = {};
      this.metricsTime = 0;
      this.stallingTasks = {};
      this.setData(clock, processors, levels, edges);
    }

    StreamingDag.prototype = {

      /** Set the current dag data */
      setData: function(clock, processors, levels, edges) {
        this.clock = clock;
        this.processors = processors;
        this.processorHierarchyLevels = levels;
        this.edges = edges;
      },

      setStallingTasks: function(tasks) {
        this.stallingTasks = tasks;
      },

      /** Update (or add) a set of metrics */
      updateMetrics: function(metrics) {
        var that = this;
        var timeMax = 0;
        _.forEach(metrics, function(metricsGroup, clazz) {
          _.forEach(metricsGroup, function(processorMetrics, processorId) {
            if (that.processors.hasOwnProperty(processorId)) {
              var metric = _.last(processorMetrics);
              that.metrics[processorId] = that.metrics[processorId] || {};
              that.metrics[processorId][clazz] = metric.values;
              timeMax = Math.max(timeMax, metric.time);
            }
          });
        });

        if (timeMax > 0) {
          this.metricsTime = timeMax;
        }
      },

      _getMetricFieldOrElse: function(processorId, clazz, field, fallback) {
        try {
          return this.metrics[processorId][clazz][field];
        } catch (ex) {
          return fallback;
        }
      },

      hasMetrics: function() {
        return this.metricsTime > 0;
      },

      /** Return the number of tasks (executed by executors). */
      getNumOfTasks: function() {
        return d3.sum(
          _.map(this._getAliveProcessors(), function(processor) {
            return processor.parallelism;
          }));
      },

      /** Return the number of (alive) processors. */
      getNumOfProcessors: function() {
        return Object.keys(this._getAliveProcessors()).length;
      },

      _getAliveProcessors: function() {
        var result = {};
        _.forEach(this.processors, function(processor, key) {
          if (processor.hasOwnProperty('life')) {
            var life = processor.life;
            if (life.hasOwnProperty('death') && this.clock > life.death) {
              return; // dead processors, drop
            }
            // (life.hasOwnProperty('birth') && this.clock < life.birth)
            // future processors, keep
          }
          result[key] = processor;
        }, this);
        return result;
      },

      _getAliveProcessorIds: function() {
        return _keysAsNum(this._getAliveProcessors());
      },

      _getProcessorEdges: function(processors) {
        var result = {};
        _.forEach(this.edges, function(edge, edgeId) {
          if (processors.hasOwnProperty(edge.source) &&
            processors.hasOwnProperty(edge.target)) {
            result[edgeId] = edge;
          }
        });
        return result;
      },

      /** Return the current dag information for drawing a DAG graph. */
      getCurrentDag: function() {
        var weights = {};
        var processors = this._getAliveProcessors();
        _.forEach(processors, function(_, key) {
          var processorId = parseInt(key); // JavaScript object key type is always string
          weights[processorId] = this._calculateProcessorWeight(processorId);
          processors[key].isStalled = this.stallingTasks.hasOwnProperty(processorId);
        }, this);

        var bandwidths = {};
        var edges = this._getProcessorEdges(processors);
        _.forEach(edges, function(_, edgeId) {
          bandwidths[edgeId] = this._calculateEdgeBandwidth(edgeId);
        }, this);

        return {
          processors: processors,
          edges: edges,
          hierarchyLevels: angular.copy(this.processorHierarchyLevels),
          weights: weights,
          bandwidths: bandwidths
        };
      },

      /** Weight of a processor equals the sum of its send throughput and receive throughput. */
      _calculateProcessorWeight: function(processorId) {
        return Math.max(
          this._getMetricFieldOrElse(processorId, 'sendThroughput', 'movingAverage1m', 0),
          this._getMetricFieldOrElse(processorId, 'receiveThroughput', 'movingAverage1m', 0)
        );
      },

      /** Bandwidth of an edge equals the minimum of average send throughput and average receive throughput. */
      _calculateEdgeBandwidth: function(edgeId) {
        var digits = edgeId.split('_');
        var sourceId = parseInt(digits[0]);
        var targetId = parseInt(digits[1]);
        var sourceOutputs = this.calculateProcessorConnections(sourceId).outputs;
        var targetInputs = this.calculateProcessorConnections(targetId).inputs;
        var sourceSendThroughput = this._getMetricFieldOrElse(sourceId, 'sendThroughput', 'movingAverage1m', 0);
        var targetReceiveThroughput = this._getMetricFieldOrElse(targetId, 'receiveThroughput', 'movingAverage1m', 0);
        return Math.min(
          sourceOutputs === 0 ? 0 : Math.round(sourceSendThroughput / sourceOutputs),
          targetInputs === 0 ? 0 : Math.round(targetReceiveThroughput / targetInputs)
        );
      },

      /** Return the number of inputs and outputs of a processor */
      calculateProcessorConnections: function(processorId) {
        var result = {inputs: 0, outputs: 0};
        var aliveProcessors = this._getAliveProcessors();
        _.forEach(this._getProcessorEdges(aliveProcessors), function(edge) {
          if (edge.source === processorId) {
            result.outputs++;
          } else if (edge.target === processorId) {
            result.inputs++;
          }
        }, /* scope */ this);
        return result;
      },

      /** Return total received messages from nodes without any outputs. */
      getReceivedMessages: function() {
        return this._getProcessingMessageThroughput(/*send*/false);
      },

      /** Return total sent messages from nodes without any inputs. */
      getSentMessages: function() {
        return this._getProcessingMessageThroughput(/*send*/true);
      },

      _getProcessingMessageThroughput: function(send) {
        var clazz = send ? 'sendThroughput' : 'receiveThroughput';
        var processorIds = this._getProcessorIdsByType(send ? 'source' : 'sink');
        return this._getProcessorThroughputAggregated(clazz, processorIds);
      },

      /** Return the aggregated throughput data of particular processors. */
      _getProcessorThroughputAggregated: function(clazz, processorIds) {
        var total = [], rate = [];
        _.forEach(processorIds, function(processorId) {
          total.push(this._getMetricFieldOrElse(processorId, clazz, 'count', 0));
          rate.push(this._getMetricFieldOrElse(processorId, clazz, 'movingAverage1m', 0));
        }, this);
        return {total: d3.sum(total), rate: d3.sum(rate)};
      },

      /** Return processor ids as an array by type (source|sink). */
      _getProcessorIdsByType: function(type) {
        return _.filter(this._getAliveProcessorIds(), function(processorId) {
          var conn = this.calculateProcessorConnections(processorId);
          return (type === 'source' && conn.inputs === 0 && conn.outputs > 0) ||
            (type === 'sink' && conn.inputs > 0 && conn.outputs === 0);
        }, this);
      },

      /** Return the average message processing time. */
      getMessageProcessingTime: function() {
        return this._getMetricFieldAverage('processTime', 'mean', 0);
      },

      /** Return the average message receive latency. */
      getMessageReceiveLatency: function() {
        return this._getMetricFieldAverage('receiveLatency', 'mean', 0);
      },

      /** Return the average value of particular metrics field of particular processor (or all processors). */
      _getMetricFieldAverage: function(clazz, field, fallback) {
        var array = _.map(this._getAliveProcessorIds(), function(processorId) {
          return this._getMetricFieldOrElse(processorId, clazz, field, fallback);
        }, this);
        return d3.mean(array);
      },

      /** Return the historical message receive throughput as an array. */
      toHistoricalMessageReceiveThroughputData: function(metrics, timeResolution) {
        var processorIds = this._getProcessorIdsByType('sink');
        return this._getProcessorHistoricalMetrics(processorIds, timeResolution,
          metrics['receiveThroughput'], 'movingAverage1m', d3.sum);
      },

      /** Return the historical message send throughput as an array. */
      toHistoricalMessageSendThroughputData: function(metrics, timeResolution) {
        var processorIds = this._getProcessorIdsByType('source');
        return this._getProcessorHistoricalMetrics(processorIds, timeResolution,
          metrics['sendThroughput'], 'movingAverage1m', d3.sum);
      },

      /** Return the historical average message processing time as an array. */
      toHistoricalMessageAverageProcessingTimeData: function(metrics, timeResolution) {
        var processorIds = _keysAsNum(this.processors);
        return this._getProcessorHistoricalMetrics(processorIds, timeResolution,
          metrics['processTime'], 'mean', d3.mean);
      },

      /** Return the historical average message receive latency as an array. */
      toHistoricalAverageMessageReceiveLatencyData: function(metrics, timeResolution) {
        var processorIds = _keysAsNum(this.processors);
        return this._getProcessorHistoricalMetrics(processorIds, timeResolution,
          metrics['receiveLatency'], 'mean', d3.mean);
      },

      /** Return particular historical metrics value of processor's tasks as an associative array. */
      _getProcessorHistoricalMetrics: function(processorIds, timeResolution, metrics, field, fn) {
        var result = {};
        _.forEach(metrics, function(processorMetrics, processorId) {
          if (_.includes(processorIds, Number(processorId))) {
            _.forEach(processorMetrics, function(metric) {
              var retainIntervalEndTime = Math.floor(metric.time / timeResolution) * timeResolution;
              result[retainIntervalEndTime] = result[retainIntervalEndTime] || [];
              result[retainIntervalEndTime].push(metric.values[field]);
            });
          }
        });

        _.forEach(result, function(array, time) {
          result[time] = _.isFunction(fn) ? fn(array) : array;
        });
        return _keysSortedObject(result);
      },

      /** Return the depth of the hierarchy layout */
      hierarchyDepth: function() {
        return d3.max(d3.values(this.processorHierarchyLevels));
      }
    };

    function _keysAsNum(object) {
      return _.map(_.keys(object), Number); // JavaScript object key type is always string
    }

    function _keysSortedObject(object) {
      var result = {};
      _.forEach(_.keys(object).sort(), function(key) {
        result[key] = object[key];
      });
      return result;
    }

    return StreamingDag;
  })
;