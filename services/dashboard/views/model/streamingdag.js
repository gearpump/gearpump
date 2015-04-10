/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('dashboard.streamingdag', ['dashboard.metrics'])

  .service('StreamingDag', ['Metrics', function (Metrics) {

    /** The constructor */
    function StreamingDag(appId, processors, levels, edges) {
      this.appId = appId;
      this.processors = _flatMap(processors); // TODO: Try and convert to Scala (#458)
      this.processorHierarchyLevels = _flatMap(levels);
      this.edges = _flatMap(edges, function (item) {
        return [item[0] + '_' + item[2], {source: item[0], target: item[2], type: item[1]}];
      });
      this.meter = {};
      this.histogram = {};
    }

    StreamingDag.prototype = {

      /** Update (or add) specified metrics in an array */
      updateMetricsArray: function (array) {
        for (var i = 0; i < array.length; i++) {
          var value = array[i].value;
          this.updateMetrics(value[0], value[1]);
        }
      },

      /** Update (or add) specified metrics */
      updateMetrics: function (name, data) {
        switch (name) {
          case 'org.apache.gearpump.metrics.Metrics.Meter':
            _update(Metrics.decodeMeter, this.meter, this);
            break;
          case 'org.apache.gearpump.metrics.Metrics.Histogram':
            _update(Metrics.decodeHistogram, this.histogram, this);
            break;
        }

        function _update(decodeFn, coll, self) {
          var metric = decodeFn(data);
          if (metric.name.appId === self.appId) {
            var item = _getOrCreate(coll, metric.name.metric, {});
            var key = metric.name.processorId + '_' + metric.name.taskId;
            item[key] = metric.values;
            item[key].processorId = metric.name.processorId;
            item[key].taskClass = self.processors[metric.name.processorId].taskClass;
            item[key].taskId = metric.name.taskId;
          }
        }
      },

      hasMetrics: function () {
        return Object.keys(this.meter).length + Object.keys(this.histogram).length > 0;
      },

      getProcessorsData: function () {
        var weights = {};
        angular.forEach(this.processors, function (_, key) {
          var processorId = parseInt(key);
          weights[processorId] = this._calculateProcessorWeight(processorId);
        }, this);
        return {
          processors: angular.copy(this.processors),
          hierarchyLevels: angular.copy(this.processorHierarchyLevels),
          weights: weights
        };
      },

      /** Weight of a processor equals the sum of its send throughput and receive throughput. */
      _calculateProcessorWeight: function (processorId) {
        var weight = 0;
        //var connections = this._calculateProcessorConnections(processorId);
        return Math.max(d3.sum(this._getProcessorMetrics(processorId, this.meter.sendThroughput, 'meanRate')),
          d3.sum(this._getProcessorMetrics(processorId, this.meter.receiveThroughput, 'meanRate')));
        //return weight;
      },

      getEdgesData: function () {
        var bandwidths = {};
        angular.forEach(this.edges, function (_, edgeId) {
          bandwidths[edgeId] = this._calculateEdgeBandwidth(edgeId);
        }, /* scope */ this);
        return {
          edges: angular.copy(this.edges),
          bandwidths: bandwidths
        };
      },

      /** Bandwidth of an edge equals the minimum of average send throughput and average receive throughput. */
      _calculateEdgeBandwidth: function (edgeId) {
        var digits = edgeId.split('_');
        var sourceId = parseInt(digits[0]);
        var targetId = parseInt(digits[1]);
        var sourceOutputs = this.calculateProcessorConnections(sourceId).outputs;
        var targetInputs = this.calculateProcessorConnections(targetId).inputs;
        var sourceSendThroughput = d3.sum(this._getProcessorMetrics(sourceId, this.meter.sendThroughput, 'meanRate'));
        var targetReceiveThroughput = d3.sum(this._getProcessorMetrics(targetId, this.meter.receiveThroughput, 'meanRate'));
        return Math.min(
          sourceOutputs === 0 ? 0 : (sourceSendThroughput / sourceOutputs),
          targetInputs === 0 ? 0 : (targetReceiveThroughput / targetInputs));
      },

      /** Returns the number of inputs and outputs of a processor */
      calculateProcessorConnections: function (processorId) {
        var result = {inputs: 0, outputs: 0};
        angular.forEach(this.edges, function (edge) {
          if (edge.source === processorId) {
            result.outputs++;
          } else if (edge.target === processorId) {
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
          var ids = processorId ? [processorId] : d3.keys(this.processors);
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
            this._getFilteredProcessorsId({hasOutputs: false})
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
            this._getFilteredProcessorsId({hasOutputs: false})
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
        var taskRateArray = this._getAggregatedMetrics(metricsGroup, 'meanRate', processorId);
        return aggregated ?
          {total: d3.sum(taskCountArray), rate: d3.sum(taskRateArray)} :
          {total: taskCountArray, rate: taskRateArray};
      },

      _getFilteredProcessorsId: function(filter) {
        var ids = [];
        d3.keys(this.processors).map(function(processorId) {
          var connections = this.calculateProcessorConnections(processorId);
          if (filter.hasOutputs && connections.outputs > 0) { return; }
          if (filter.hasInputs && connections.inputs > 0) { return; }
          ids.push(processorId);
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