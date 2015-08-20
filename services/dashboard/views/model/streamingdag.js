/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('dashboard.streamingdag', ['dashboard.metrics'])

  .service('StreamingDag', ['Metrics', function (Metrics) {

    /** The constructor */
    function StreamingDag(appId, clock, processors, levels, edges, executors) {
      this.appId = appId;
      this.meter = {};
      this.histogram = {};
      this.stalling = {};
      this.setData(clock, processors, levels, edges, executors);
    }

    StreamingDag.prototype = {

      /** Update the current dag data */
      setData: function(clock, processors, levels, edges, executors) {
        this.clock = clock;
        this.processors = _flatMap(processors); // TODO: Try and convert to Scala (#458)
        this.processorHierarchyLevels = _flatMap(levels);
        this.edges = _flatMap(edges, function (item) {
          return [item[0] + '_' + item[2], {source: item[0], target: item[2], type: item[1]}];
        });
        this.executors = _flatMap(executors);
      },

      setStallingProcessors: function(ids) {
        this.stalling = ids;
      },

      /** Update (or add) specified metrics in an array */
      updateMetricsArray: function (array) {
        for (var i = 0; i < array.length; i++) {
          var value = array[i].value;
          this.updateMetrics(value.$type, value);
        }
      },

      /** Update (or add) specified metrics */
      updateMetrics: function (name, data) {
        switch (name) {
          case 'io.gearpump.metrics.Metrics.Meter':
            _update(Metrics.decodeMeter, this.meter, this);
            break;
          case 'io.gearpump.metrics.Metrics.Histogram':
            _update(Metrics.decodeHistogram, this.histogram, this);
            break;
        }

        function _update(decodeFn, coll, self) {
          var metric = decodeFn(data);
          if (metric.name.appId === self.appId) {
            if (!(metric.name.processorId in self.processors)) {
              return;
            }
            var item = _getOrCreate(coll, metric.name.metric, {});
            var key = metric.name.processorId + '_' + metric.name.taskId;
            item[key] = metric.values;
            item[key].processorId = metric.name.processorId;
            item[key].taskClass = self.processors[metric.name.processorId].taskClass;
            item[key].taskId = metric.name.taskId;
            item[key].taskPath = 'processor' + metric.name.processorId + '.task' + metric.name.taskId;
          }
        }
      },

      hasMetrics: function () {
        return Object.keys(this.meter).length + Object.keys(this.histogram).length > 0;
      },

      getNumOfTasks: function() {
        var count = 0;
        angular.forEach(this.getAliveProcessors(), function(processor) {
          count += processor.parallelism;
        }, this);
        return count;
      },

      getAliveProcessors: function() {
        var result = {};
        angular.forEach(this.processors, function(processor, processorId) {
          if (processor.hasOwnProperty('life')) {
            var life = processor.life;
            if (life.hasOwnProperty('death') && this.clock > life.death) {
              return; // dead processors, drop
            }
            // (life.hasOwnProperty('birth') && this.clock < life.birth)
            // future processors, keep
          }
          result[processorId] = processor;
        }, this);
        return result;
      },

      getAliveEdges: function(aliveProcessors) {
        if (!aliveProcessors) {
          aliveProcessors = this.getAliveProcessors();
        }
        var result = {};
        angular.forEach(this.edges, function(edge, edgeId) {
          if (edge.source in aliveProcessors && edge.target in aliveProcessors) {
            result[edgeId] = edge;
          }
        }, this);
        return result;
      },

      getCurrentDag: function () {
        var weights = {};
        var processors = angular.copy(this.getAliveProcessors());
        angular.forEach(processors, function(_, key) {
          var processorId = parseInt(key);
          weights[processorId] = this._calculateProcessorWeight(processorId);
          processors[key].stalling = processorId in this.stalling;
        }, this);

        var bandwidths = {};
        var edges = this.getAliveEdges(processors);
        angular.forEach(edges, function (_, edgeId) {
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
      _calculateProcessorWeight: function (processorId) {
        return Math.max(d3.sum(this._getProcessorMetrics(processorId, this.meter.sendThroughput, 'movingAverage1m')),
          d3.sum(this._getProcessorMetrics(processorId, this.meter.receiveThroughput, 'movingAverage1m')));
      },

      /** Bandwidth of an edge equals the minimum of average send throughput and average receive throughput. */
      _calculateEdgeBandwidth: function (edgeId) {
        var digits = edgeId.split('_');
        var sourceId = parseInt(digits[0]);
        var targetId = parseInt(digits[1]);
        var sourceOutputs = this.calculateProcessorConnections(sourceId).outputs;
        var targetInputs = this.calculateProcessorConnections(targetId).inputs;
        var sourceSendThroughput = d3.sum(this._getProcessorMetrics(sourceId, this.meter.sendThroughput, 'movingAverage1m'));
        var targetReceiveThroughput = d3.sum(this._getProcessorMetrics(targetId, this.meter.receiveThroughput, 'movingAverage1m'));
        return Math.min(
          sourceOutputs === 0 ? 0 : (sourceSendThroughput / sourceOutputs),
          targetInputs === 0 ? 0 : (targetReceiveThroughput / targetInputs));
      },

      /** Returns the number of inputs and outputs of a processor */
      calculateProcessorConnections: function (processorId) {
        var result = {inputs: 0, outputs: 0};
        angular.forEach(this.edges, function (edge) {
          if (edge.source == processorId) {
            result.outputs++;
          } else if (edge.target == processorId) {
            result.inputs++;
          }
        }, /* scope */ this);
        return result;
      },

      /** Return particular metrics value of a processor as an array. */
      _getProcessorMetrics: function (processorId, metricsGroup, metricType) {
        var values = [];
        if (metricsGroup) {
          var tasks = this.processors[processorId].parallelism;
          for (var taskId = 0; taskId < tasks; taskId++) {
            var name = processorId + '_' + taskId;
            if (metricsGroup.hasOwnProperty(name)) {
              values.push(metricsGroup[name][metricType]);
            }
          }
        }
        return values;
      },

      /** Return particular metrics value of all processors as an array. */
      _getAggregatedMetrics: function (metricsGroup, metricsType, processorId) {
        var values = [];
        if (metricsGroup) {
          var ids = processorId !== undefined ? [processorId] : d3.keys(this.getAliveProcessors());
          ids.map(function (processorId) {
            var processorValues = this._getProcessorMetrics(processorId, metricsGroup, metricsType);
            values = values.concat(processorValues);
          }, /* scope */ this);
        }
        return values;
      },

      /** Return total received messages from nodes without any outputs. */
      getReceivedMessages: function(processorId) {
        if (processorId !== undefined) {
          return this._getProcessedMessagesByProcessor(
            this.meter.receiveThroughput, processorId, false
          );
        } else {
          return this._getProcessedMessages(
            this.meter.receiveThroughput,
            this._getProcessorIdsByType('sink')
          );
        }
      },

      /** Return total sent messages from nodes without any inputs. */
      getSentMessages: function(processorId) {
        if (processorId !== undefined) {
          return this._getProcessedMessagesByProcessor(
            this.meter.sendThroughput, processorId, false
          );
        } else {
          return this._getProcessedMessages(
            this.meter.sendThroughput,
            this._getProcessorIdsByType('source')
          );
        }
      },

      _getProcessedMessages: function(metricsGroup, processorIds) {
        var result = {total: [], rate: []};
        processorIds.map(function(processorId) {
          var processorResult = this._getProcessedMessagesByProcessor(
            metricsGroup, processorId, true);
          result.total.push(processorResult.total);
          result.rate.push(processorResult.rate);
        }, /* scope */ this);
        return {total: d3.sum(result.total), rate: d3.sum(result.rate)};
      },

      _getProcessedMessagesByProcessor: function(metricsGroup, processorId, aggregated) {
        var taskCountArray = this._getAggregatedMetrics(metricsGroup, 'count', processorId);
        var taskRateArray = this._getAggregatedMetrics(metricsGroup, 'movingAverage1m', processorId);
        return aggregated ?
          {total: d3.sum(taskCountArray), rate: d3.sum(taskRateArray)} :
          {total: taskCountArray, rate: taskRateArray};
      },

      _getProcessorIdsByType: function(type) {
        var ids = [];
        d3.keys(this.getAliveProcessors()).map(function(processorId) {
          var conn = this.calculateProcessorConnections(processorId);
          if ((type === 'source' && conn.inputs === 0 && conn.outputs > 0) ||
            (type === 'sink' && conn.inputs > 0 && conn.outputs === 0)) {
            ids.push(processorId);
          }
        }, /* scope */ this);
        return ids;
      },

      getProcessingTime: function (processorId) {
        var array = this._getAggregatedMetrics(this.histogram.processTime, 'mean', processorId);
        return processorId !== undefined ? array : d3.mean(array);
      },

      getReceiveLatency: function (processorId) {
        var array = this._getAggregatedMetrics(this.histogram.receiveLatency, 'mean', processorId);
        return processorId !== undefined ? array : d3.mean(array);
      },

      /** Return the depth of the hierarchy layout */
      hierarchyDepth: function () {
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
      array.map(function (item) {
        if (fn) {
          item = fn(item);
        }
        result[item[0]] = item[1];
      });
      return result;
    }

    return StreamingDag;
  }])
;