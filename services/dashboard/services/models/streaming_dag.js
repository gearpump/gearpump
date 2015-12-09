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
      // Metrics are stored in this hierarchy:
      //   group: meter|histogram|gauge
      //     name: sendThroughput|receiveThroughput|etc.
      //       field: count|movingAverage1M|etc.
      this.metrics = {meter: {}, histogram: {}, gauge: {}};
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

      /** Update (or add) an array of metrics */
      updateMetricsArray: function(array) {
        _.forEach(array, function(metric) {
          if (metric.isMeter) {
            this._updateProcessorMetrics('meter', metric);
          } else if (metric.isHistogram) {
            this._updateProcessorMetrics('histogram', metric);
          } else {
            console.warn({message: 'Unhandled metric', metric: metric});
          }
        }, this);
      },

      _updateProcessorMetrics: function(metricsGroupName, metric) {
        if (!(metric.meta.processorId in this.processors)) {
          console.warn({message: 'The metric is reported by an unknown processor', metric: metric});
          return;
        }

        var path = metric.meta.processorId + '_' + metric.meta.taskId;
        var value = {
          processorId: metric.meta.processorId,
          taskId: metric.meta.taskId,
          taskClass: this.processors[metric.meta.processorId].taskClass,
          values: metric.values
        };

        var metricGroupCurrent = this.metrics[metricsGroupName];
        var shorthandAccessor = _getOrCreate(metricGroupCurrent, metric.meta.clazz, {});
        // example path:         [root] [group]    [clazz]   [path]
        //                 "this.metrics.meter.sendThroughput.0_0"
        shorthandAccessor[path] = value;
        metricGroupCurrent.time = metric.time;
      },

      hasMetrics: function() {
        return this.metrics.meter.time || this.metrics.histogram.time;
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
          if (edge.source in processors && edge.target in processors) {
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
          processors[key].isStalled = key in this.stallingTasks;
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
          d3.sum(this._getProcessorMetrics(processorId, this.metrics.meter['sendThroughput'], 'movingAverage1m')),
          d3.sum(this._getProcessorMetrics(processorId, this.metrics.meter['receiveThroughput'], 'movingAverage1m'))
        );
      },

      /** Bandwidth of an edge equals the minimum of average send throughput and average receive throughput. */
      _calculateEdgeBandwidth: function(edgeId) {
        var digits = edgeId.split('_');
        var sourceId = parseInt(digits[0]);
        var targetId = parseInt(digits[1]);
        var sourceOutputs = this.calculateProcessorConnections(sourceId).outputs;
        var targetInputs = this.calculateProcessorConnections(targetId).inputs;
        var sourceSendThroughput = d3.sum(this._getProcessorMetrics(sourceId, this.metrics.meter['sendThroughput'], 'movingAverage1m'));
        var targetReceiveThroughput = d3.sum(this._getProcessorMetrics(targetId, this.metrics.meter['receiveThroughput'], 'movingAverage1m'));
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

      /** Return particular metrics value of processor's tasks as an array. */
      _getProcessorMetrics: function(processorId, metricsGroup, metricField) {
        var array = [];
        if (metricsGroup) {
          var parallelism = this.processors[processorId].parallelism;
          for (var taskId = 0; taskId < parallelism; taskId++) {
            var name = processorId + '_' + taskId;
            if (metricsGroup.hasOwnProperty(name)) {
              var metric = metricsGroup[name];
              array.push(metric.values[metricField]);
            }
          }
        }
        return array;
      },

      /** Return particular metrics value of particular processor (or all processors) as an array. */
      _getProcessorOrAllMetricsAsArray: function(metricsGroup, metricField, processorId) {
        var array = [];
        var processorIds = processorId !== undefined ?
          [processorId] : this._getAliveProcessorIds();
        _.forEach(processorIds, function(processorId) {
          var processorResult = this._getProcessorMetrics(processorId, metricsGroup, metricField);
          array = array.concat(processorResult);
        }, this);
        return array;
      },

      /** Return total received messages from nodes without any outputs. */
      getReceivedMessages: function(processorId) {
        return this._getProcessingMessageThroughput(/*send*/false, processorId);
      },

      /** Return total sent messages from nodes without any inputs. */
      getSentMessages: function(processorId) {
        return this._getProcessingMessageThroughput(/*send*/true, processorId);
      },

      _getProcessingMessageThroughput: function(send, processorId) {
        var metricsGroup = this.metrics.meter[send ? 'sendThroughput' : 'receiveThroughput'];
        if (processorId !== undefined) {
          return this._getProcessorTaskThroughput(metricsGroup, processorId);
        } else {
          var processorIds = this._getProcessorIdsByType(send ? 'source' : 'sink');
          return this._getProcessorThroughputAggregated(metricsGroup, processorIds);
        }
      },

      /** Return the aggregated throughput data of particular processors. */
      _getProcessorThroughputAggregated: function(throughputMetricsGroup, processorIds) {
        var total = [], rate = [];
        _.forEach(processorIds, function(processorId) {
          var processorResult = this._getProcessorTaskThroughput(
            throughputMetricsGroup, processorId);
          total.push(d3.sum(processorResult.total));
          rate.push(d3.sum(processorResult.rate));
        }, this);
        return {total: d3.sum(total), rate: d3.sum(rate)};
      },

      /** Return the throughput data of particular processor tasks as associative array. */
      _getProcessorTaskThroughput: function(throughputMetricsGroup, processorId) {
        return {
          total: this._getProcessorMetrics(processorId, throughputMetricsGroup, 'count'),
          rate: this._getProcessorMetrics(processorId, throughputMetricsGroup, 'movingAverage1m')
        };
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
      getMessageProcessingTime: function(processorId) {
        return this._getMetricFieldArrayOrAverage(
          this.metrics.histogram['processTime'], 'mean', processorId);
      },

      /** Return the average message receive latency. */
      getMessageReceiveLatency: function(processorId) {
        return this._getMetricFieldArrayOrAverage(
          this.metrics.histogram['receiveLatency'], 'mean', processorId);
      },

      /** Return the average value of particular metrics field of particular processor (or all processors). */
      _getMetricFieldArrayOrAverage: function(metricsGroup, metricField, processorId) {
        var array = this._getProcessorOrAllMetricsAsArray(metricsGroup, metricField, processorId);
        return processorId !== undefined ?
          array : (d3.mean(array) || 0);
      },

      /** Return the historical message receive throughput as an array. */
      toHistoricalMessageReceiveThroughputData: function(metrics) {
        var processorIds = this._getProcessorIdsByType('sink');
        var metricsGroup = metrics['receiveThroughput'];
        var metricField = 'movingAverage1m';
        return this._getProcessorHistoricalMetrics(processorIds, metricsGroup, metricField, d3.sum);
      },

      /** Return the historical message send throughput as an array. */
      toHistoricalMessageSendThroughputData: function(metrics) {
        var processorIds = this._getProcessorIdsByType('source');
        var metricsGroup = metrics['sendThroughput'];
        var metricField = 'movingAverage1m';
        return this._getProcessorHistoricalMetrics(processorIds, metricsGroup, metricField, d3.sum);
      },

      /** Return the historical average message processing time as an array. */
      toHistoricalMessageAverageProcessingTimeData: function(metrics) {
        var processorIds = _keysAsNum(this.processors);
        var metricsGroup = metrics['processTime'];
        var metricField = 'mean';
        return this._getProcessorHistoricalMetrics(processorIds, metricsGroup, metricField, d3.mean);
      },

      /** Return the historical average message receive latency as an array. */
      toHistoricalAverageMessageReceiveLatencyData: function(metrics) {
        var processorIds = _keysAsNum(this.processors);
        var metricsGroup = metrics['receiveLatency'];
        var metricField = 'mean';
        return this._getProcessorHistoricalMetrics(processorIds, metricsGroup, metricField, d3.mean);
      },

      /** Return particular historical metrics value of processor's tasks as an associative array. */
      _getProcessorHistoricalMetrics: function(processorIds, metricsGroup, metricField, fn) {
        var result = {};
        if (metricsGroup) {
          // Calculate names once before enter the time consuming iterations
          var paths = [];
          _.forEach(processorIds, function(processorId) {
            var parallelism = this.processors[processorId].parallelism;
            for (var taskId = 0; taskId < parallelism; taskId++) {
              var path = 'processor' + processorId + '.task' + taskId;
              paths.push(path);
            }
          }, this);

          // Iterate historical metrics by time
          _.forEach(metricsGroup, function(metrics, path) {
            path = path.split('.').slice(1).join('.'); // strip the prefix 'app0.'
            if (paths.indexOf(path) !== -1) {
              _.forEach(metrics, function(metric) {
                result[metric.time] = result[metric.time] || [];
                result[metric.time].push((metric.hasOwnProperty('values') ?
                  metric.values : metric.value)[metricField]);
              });
            }
          });

          _.forEach(result, function(array, time) {
            result[time] = _.isFunction(fn) ? fn(array) : array;
          });
        }
        return result;
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

    function _keysAsNum(object) {
      return _.map(Object.keys(object), function(key) {
        return Number(key); // JavaScript object key type is always string
      });
    }

    return StreamingDag;
  })
;