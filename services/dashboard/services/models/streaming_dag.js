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
      this.metrics = {meter: {}, histogram: {}};
      this.metricsHistory = {meter: {}, histogram: {}};
      this.stallingTasks = {};
      this.setData(clock, processors, levels, edges);
    }

    StreamingDag.prototype = {

      /** Set the current dag data */
      setData: function(clock, processors, levels, edges) {
        this.clock = clock;
        this.processors = _flatMap(processors); // TODO: Try and convert to Scala (#458)
        this.processorHierarchyLevels = _flatMap(levels);
        this.edges = _flatMap(edges, function(item) {
          return [item[0] + '_' + item[2],
            {source: parseInt(item[0]), target: parseInt(item[2]), type: item[1]}];
        });
      },

      setStallingTasks: function(tasks) {
        this.stallingTasks = tasks;
      },

      /** Update (or add) an array of metrics */
      updateMetricsArray: function(array) {
        var reportDuplicates = !this.hasMetrics();
        _.forEach(array, function(metric) {
          if (metric.isMeter) {
            this._updateProcessorMetrics('meter', metric, reportDuplicates);
          } else if (metric.isHistogram) {
            this._updateProcessorMetrics('histogram', metric, reportDuplicates);
          } else if (metric.isGauge) {
            this._updateApplicationMetrics('gauge', metric);
          }
        }, this);
      },

      _updateProcessorMetrics: function(metricsGroupName, metric, reportDuplicates) {
        var processors = this._getAliveProcessors();
        if (!(metric.meta.processorId in processors)) {
          console.warn({message: 'Only alive processor metrics will be taken', metric: metric});
          return;
        }

        var key = metric.meta.processorId + '_' + metric.meta.taskId;
        var value = angular.merge({}, metric.values, {
          processorId: metric.meta.processorId,
          taskId: metric.meta.taskId,
          taskClass: processors[metric.meta.processorId].taskClass,
          taskPath: 'processor' + metric.meta.processorId + '.task' + metric.meta.taskId
        });

        var metricGroupCurrent = this.metrics[metricsGroupName];
        var shorthandAccessor = _getOrCreate(metricGroupCurrent, metric.meta.clazz, {});
        // example path:   [metricGroupCurrent] [meta.clazz] [key]
        //                 "this.metrics.meter.sendThroughput.0_0"
        shorthandAccessor[key] = value;
        metricGroupCurrent.time = metric.time;

        var metricGroupHistory = this.metricsHistory[metricsGroupName];
        shorthandAccessor = _getOrCreate(metricGroupHistory, metric.meta.clazz, {});
        shorthandAccessor = _getOrCreate(shorthandAccessor, key, {});
        if (metric.time in shorthandAccessor) {
          if (reportDuplicates) {
            console.warn({
              message: 'Will not add a metric with the same timestamp twice',
              source: metric, target: shorthandAccessor[metric.time]
            });
          }
          return;
        }
        // example path:          [metricGroupHistory] [meta.clazz] [key] [time]
        //                 "this.metricsHistory.meter.sendThroughput.0_0.14000000000"
        shorthandAccessor[metric.time] = metric.values;
        // todo: rotation by removing metrics with fresh timestamp
      },

      _updateApplicationMetrics: function(metricsGroupName, metric) {
        // todo:
      },

      hasMetrics: function() {
        return this.metrics.meter.time || this.metrics.histogram.time;
      },

      getNumOfTasks: function() {
        var count = 0;
        _.forEach(this._getAliveProcessors(), function(processor) {
          count += processor.parallelism;
        }, this);
        return count;
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
        return _.map(Object.keys(this._getAliveProcessors()), function(key) {
          return parseInt(key); // JavaScript object key type is always string
        });
      },

      _getAliveEdges: function(aliveProcessors) {
        var result = {};
        _.forEach(this.edges, function(edge, edgeId) {
          if (edge.source in aliveProcessors && edge.target in aliveProcessors) {
            result[edgeId] = edge;
          }
        }, this);
        return result;
      },

      getCurrentDag: function() {
        var weights = {};
        var processors = this._getAliveProcessors();
        _.forEach(processors, function(_, key) {
          var processorId = parseInt(key); // JavaScript object key type is always string
          weights[processorId] = this._calculateProcessorWeight(processorId);
          processors[key].stalling = processorId in this.stallingTasks;
        }, this);

        var bandwidths = {};
        var edges = this._getAliveEdges(processors);
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
          d3.sum(this._getProcessorMetrics(processorId, this.metrics.meter.sendThroughput, 'movingAverage1m')),
          d3.sum(this._getProcessorMetrics(processorId, this.metrics.meter.receiveThroughput, 'movingAverage1m')));
      },

      /** Bandwidth of an edge equals the minimum of average send throughput and average receive throughput. */
      _calculateEdgeBandwidth: function(edgeId) {
        var digits = edgeId.split('_');
        var sourceId = parseInt(digits[0]);
        var targetId = parseInt(digits[1]);
        var sourceOutputs = this.calculateProcessorConnections(sourceId).outputs;
        var targetInputs = this.calculateProcessorConnections(targetId).inputs;
        var sourceSendThroughput = d3.sum(this._getProcessorMetrics(sourceId, this.metrics.meter.sendThroughput, 'movingAverage1m'));
        var targetReceiveThroughput = d3.sum(this._getProcessorMetrics(targetId, this.metrics.meter.receiveThroughput, 'movingAverage1m'));
        return Math.min(
          sourceOutputs === 0 ? 0 : Math.round(sourceSendThroughput / sourceOutputs),
          targetInputs === 0 ? 0 : Math.round(targetReceiveThroughput / targetInputs)
        );
      },

      /** Returns the number of inputs and outputs of a processor */
      calculateProcessorConnections: function(processorId) {
        var result = {inputs: 0, outputs: 0};
        _.forEach(this.edges, function(edge) {
          if (edge.source === processorId) {
            result.outputs++;
          } else if (edge.target === processorId) {
            result.inputs++;
          }
        }, /* scope */ this);
        return result;
      },

      /** Return particular metrics value of a processor's tasks as an array. */
      _getProcessorMetrics: function(processorId, metricsGroup, metricField) {
        var values = [];
        if (metricsGroup) {
          var parallelism = this.processors[processorId].parallelism;
          for (var taskId = 0; taskId < parallelism; taskId++) {
            var name = processorId + '_' + taskId;
            if (metricsGroup.hasOwnProperty(name)) {
              var current = metricsGroup[name];
              values.push(current[metricField]);
            }
          }
        }
        return values;
      },

      /** Result is an associative array */
      _getProcessorHistoricalMetrics: function(processorId, metricsGroup, metricField) {
        var values = {};
        if (metricsGroup) {
          var parallelism = this.processors[processorId].parallelism;
          for (var taskId = 0; taskId < parallelism; taskId++) {
            var name = processorId + '_' + taskId;
            if (metricsGroup.hasOwnProperty(name)) {
              var current = metricsGroup[name];
              values.push(current[metricField]);
            }
          }
        }
        return values;
      },

      /** Return particular metrics value of all processors as an array. */
      _getAggregatedMetrics: function(metricsGroup, metricField, processorId) {
        var values = [];
        var processorIds = processorId !== undefined ?
          [processorId] : this._getAliveProcessorIds();
        _.forEach(processorIds, function(processorId) {
          var processorValues = this._getProcessorMetrics(processorId, metricsGroup, metricField);
          values = values.concat(processorValues);
        }, this);
        return values;
      },

      /** Return total received messages from nodes without any outputs. */
      getReceivedMessages: function(processorId) {
        if (processorId !== undefined) {
          return this._getThroughputByProcessor(
            this.metrics.meter.receiveThroughput, processorId);
        } else {
          var sinkProcessorIds = this._getProcessorIdsByType('sink');
          return this._getThroughputAggregated(
            this.metrics.meter.receiveThroughput, sinkProcessorIds);
        }
      },

      getHistoricalMessageReceiveThroughput: function() {
        return this.metricsHistory.meter.receiveThroughput;
        //var sinkProcessorIds = this._getProcessorIdsByType('sink');
        //_.forEach(sinkProcessorIds, function(processorId) {
        //  this._getAggregatedMetrics(this.metrics.meter.receiveThroughput, 'movingAverage1m', processorId);
        //});
      },

      /** Return total sent messages from nodes without any inputs. */
      getSentMessages: function(processorId) {
        if (processorId !== undefined) {
          return this._getThroughputByProcessor(
            this.metrics.meter.sendThroughput, processorId);
        } else {
          var sourceProcessorIds = this._getProcessorIdsByType('source');
          return this._getThroughputAggregated(
            this.metrics.meter.sendThroughput, sourceProcessorIds);
        }
      },

      _getThroughputAggregated: function(throughputMetricsGroup, processorIds) {
        var total = [], rate = [];
        _.forEach(processorIds, function(processorId) {
          var processorResult = this._getThroughputByProcessor(
            throughputMetricsGroup, processorId);
          total.push(d3.sum(processorResult.total));
          rate.push(d3.sum(processorResult.rate));
        }, this);
        return {total: d3.sum(total), rate: d3.sum(rate)};
      },

      _getThroughputByProcessor: function(throughputMetricsGroup, processorId) {
        return {
          total: this._getAggregatedMetrics(throughputMetricsGroup, 'count', processorId),
          rate: this._getAggregatedMetrics(throughputMetricsGroup, 'movingAverage1m', processorId)
        };
      },

      _getProcessorIdsByType: function(type) {
        return _.filter(this._getAliveProcessorIds(), function(processorId) {
          var conn = this.calculateProcessorConnections(processorId);
          return (type === 'source' && conn.inputs === 0 && conn.outputs > 0) ||
            (type === 'sink' && conn.inputs > 0 && conn.outputs === 0);
        }, this);
      },

      /** Return the average message processing time. */
      getMessageProcessingTime: function(processorId) {
        var array = this._getAggregatedMetrics(this.metrics.histogram.processTime, 'mean', processorId);
        return processorId !== undefined ? array : (d3.mean(array) || 0);
      },

      /** Return the average message receive latency. */
      getMessageReceiveLatency: function(processorId) {
        var array = this._getAggregatedMetrics(this.metrics.histogram.receiveLatency, 'mean', processorId);
        return processorId !== undefined ? array : (d3.mean(array) || 0);
      },

      /** Return the depth of the hierarchy layout */
      hierarchyDepth: function() {
        return d3.max(d3.values(this.processorHierarchyLevels));
      }
    };

    function _getOrCreate(obj, prop, init) {
      if (!obj.hasOwnProperty(prop)) {
        obj[prop] = init;
      }
      return obj[prop];
    }

    function _flatMap(array, fn) {
      var result = {};
      _.forEach(array, function(item) {
        if (fn) {
          item = fn(item);
        }
        result[item[0]] = item[1];
      });
      return result;
    }

    return StreamingDag;
  })
;