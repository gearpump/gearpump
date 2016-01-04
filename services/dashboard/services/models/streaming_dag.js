/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

/** TODO: to be absorbed as scalajs */
  .service('StreamingDag', function() {
    'use strict';

    /** The constructor */
    function StreamingDag(clock, processors, edges) {
      this.metrics = {};
      this.metricsTime = 0;
      this.stallingTasks = {};
      this.setData(clock, processors, edges);
    }

    StreamingDag.prototype = {

      /** Set the current dag data */
      setData: function(clock, processors, edges) {
        this.clock = clock;
        if (!_.isEqual(this.processors, processors) || !_.isEqual(this.edges, edges)) {
          // only do calculation when topology has been changed
          this.processors = processors;
          this.edges = edges;
          this.sortedNonSourceProcessorIds = this._getTopologicalSortedNonSourceProcessIds();
          this.predecessors = this._calculatePredecessors();
        }
      },

      /** Return processor ids sorted in topological order without source processors (hierarchy=0) */
      _getTopologicalSortedNonSourceProcessIds: function() {
        return _(this.processors).filter(function(processor) {
          return processor.hierarchy > 0;
        })
          .sortBy('hierarchy')
          .pluck('id')
          .value();
      },

      _calculatePredecessors: function() {
        var result = {};
        _.forEach(this._getProcessorIds(), function(processorId) {
          result[processorId] = this._getPredecessorsIds(processorId);
        }, this);
        return result;
      },

      _getPredecessorsIds: function(processorId) {
        var result = [];
        _.forEach(this.edges, function(processorEdges, id) {
          if (id != processorId) {
            _.forEach(processorEdges, function(edge) {
              if (edge.to === processorId) {
                result.push(edge.from);
              }
            });
          }
        });
        return result;
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

      /** Return the number of tasks (executed by executors). */
      getNumOfTasks: function() {
        return _.sum(this._getActiveProcessors(), 'parallelism');
      },

      /** Return the number of current active processors. */
      getNumOfProcessors: function() {
        return _.keys(this._getActiveProcessors()).length;
      },

      _getActiveProcessors: function() {
        var result = {};
        _.forEach(this.processors, function(processor, key) {
          if (processor.active) {
            result[key] = processor;
          }
        });
        return result;
      },

      _getActiveProcessorIds: function() {
        return _keysAsNum(this._getActiveProcessors());
      },

      _getProcessorIds: function() {
        return _keysAsNum(this.processors);
      },

      _getRelatedEdges: function(processors) {
        var result = {};
        _.forEach(this.edges, function(processorEdges, processorId) {
          if (processors.hasOwnProperty(processorId)) {
            _.forEach(processorEdges, function (edge) {
              if (processors.hasOwnProperty(edge.to)) {
                result[edge.from + '_' + edge.to] = edge;
              }
            });
          }
        });
        return result;
      },

      /** Return the current dag information for drawing a DAG graph. */
      getCurrentDag: function() {
        var weights = {};
        var processors = this._getActiveProcessors();
        _.forEach(processors, function(processor, key) {
          var processorId = Number(key);
          weights[processorId] = this._calculateProcessorWeight(processorId);
          var stalledTasks = this.stallingTasks.hasOwnProperty(processorId);
          if (stalledTasks.length || processor.hasOwnProperty('stalledTasks')) {
            processor.stalledTasks = stalledTasks;
          }
        }, this);

        var bandwidths = {};
        var edges = this._getRelatedEdges(processors);
        _.forEach(edges, function(_, edgeId) {
          bandwidths[edgeId] = this._calculateEdgeBandwidth(edgeId);
        }, this);

        return {
          processors: processors,
          edges: edges,
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
        var sourceOutdegree = this.calculateProcessorDegrees(sourceId).outdegree;
        var targetIndegree = this.calculateProcessorDegrees(targetId).indegree;
        var sourceSendThroughput = this._getMetricFieldOrElse(sourceId, 'sendThroughput', 'movingAverage1m', 0);
        var targetReceiveThroughput = this._getMetricFieldOrElse(targetId, 'receiveThroughput', 'movingAverage1m', 0);
        return Math.min(
          sourceOutdegree === 0 ? 0 : Math.round(sourceSendThroughput / sourceOutdegree),
          targetIndegree === 0 ? 0 : Math.round(targetReceiveThroughput / targetIndegree)
        );
      },

      /** Return the indegree and outdegree of a processor */
      calculateProcessorDegrees: function(processorId, includeDead) {
        var result = {indegree: 0, outdegree: 0};
        var processors = includeDead ?
          this.processors : this._getActiveProcessors();
        _.forEach(this._getRelatedEdges(processors), function(edge) {
          if (edge.from === processorId) {
            result.outdegree++;
          } else if (edge.to === processorId) {
            result.indegree++;
          }
        });
        return result;
      },

      /** Return total received messages from particular processor (or all active processors) without any outputs. */
      getReceivedMessages: function(processorId) {
        return this._getMessageThroughputTotalAndRate(/*send*/false, processorId);
      },

      /** Return total sent messages from particular processor (or all active processors) without any inputs. */
      getSentMessages: function(processorId) {
        return this._getMessageThroughputTotalAndRate(/*send*/true, processorId);
      },

      _getMessageThroughputTotalAndRate: function(send, processorId) {
        var clazz = send ? 'sendThroughput' : 'receiveThroughput';
        var processorIds = angular.isNumber(processorId) ?
          [processorId] : this._getProcessorIdsByType(send ? 'source' : 'sink');
        var total = [], rate = [];
        _.forEach(processorIds, function(processorId) {
          total.push(this._getMetricFieldOrElse(processorId, clazz, 'count', 0));
          rate.push(this._getMetricFieldOrElse(processorId, clazz, 'movingAverage1m', 0));
        }, this);
        return {total: d3.sum(total), rate: d3.sum(rate)};
      },

      /** Return processor ids as an array by type (source|sink). */
      _getProcessorIdsByType: function(type, includeDead) {
        var processorIds = includeDead ?
          this._getProcessorIds() : this._getActiveProcessorIds();
        return _.filter(processorIds, function(processorId) {
          var degrees = this.calculateProcessorDegrees(processorId, includeDead);
          return (type === 'source' && degrees.indegree === 0 && degrees.outdegree > 0) ||
            (type === 'sink' && degrees.indegree > 0 && degrees.outdegree === 0);
        }, this);
      },

      /** Return the average message processing time of particular processor (or all active processors). */
      getMessageProcessingTime: function(processorId) {
        var fallback = 0;
        return angular.isNumber(processorId) ?
          this._getMetricFieldOrElse(processorId, 'processTime', 'mean', fallback) :
          this._getProcessorsMetricFieldAverage(
            this._getActiveProcessorIds(), 'processTime', 'mean', fallback);
      },

      /** Return the average message receive latency of particular processor or the latency on critical path. */
      getMessageReceiveLatency: function(processorId) {
        var fallback = 0;
        return angular.isNumber(processorId) ?
          this._getMetricFieldOrElse(processorId, 'receiveLatency', 'mean', fallback) :
          this.getCriticalPathLatency();
      },

      /** Return the average value of particular metrics field of particular processor (or all processors). */
      _getProcessorsMetricFieldAverage: function(processorIds, clazz, field, fallback) {
        var array = _.map(processorIds, function(processorId) {
          return this._getMetricFieldOrElse(processorId, clazz, field, fallback);
        }, this);
        return d3.mean(array);
      },

      /** Return the message latency on critical path. */
      getCriticalPathLatency: function() {
        var that = this;
        var latencyLookup = {};
        _.forEach(this.sortedNonSourceProcessorIds, function(processorId) {
          latencyLookup[processorId] =
            that.getMessageReceiveLatency(processorId) +
            that.getMessageProcessingTime(processorId);
        });
        _.forEach(this.sortedNonSourceProcessorIds, function(processorId) {
          var predecessorsIds = that.predecessors[processorId];
          if (predecessorsIds.length) {
            var predecessorsLatencies = _.map(predecessorsIds, function(id) {
              return latencyLookup[id] || 0;
            });
            latencyLookup[processorId] += _.max(predecessorsLatencies);
          }
        });
        return _.max(_.values(latencyLookup));
      },

      /**
       * Return the historical message receive throughput as an array. If processorId is not specified, it
       * will only return receive throughput data of data sink processors.
       */
      toHistoricalMessageReceiveThroughputData: function(metrics, timeResolution, processorId) {
        var processorIds = angular.isNumber(processorId) ?
          [processorId] : this._getProcessorIdsByType('sink', /*includeDead=*/true);
        return this._getProcessorHistoricalMetrics(processorIds, timeResolution,
          metrics['receiveThroughput'], 'movingAverage1m', d3.sum);
      },

      /**
       * Return the historical message send throughput as an array. If processorId is not specified, it
       * will only return send throughput data of data source processors.
       */
      toHistoricalMessageSendThroughputData: function(metrics, timeResolution, processorId) {
        var processorIds = angular.isNumber(processorId) ?
          [processorId] : this._getProcessorIdsByType('source', /*includeDead=*/true);
        return this._getProcessorHistoricalMetrics(processorIds, timeResolution,
          metrics['sendThroughput'], 'movingAverage1m', d3.sum);
      },

      /** Return the historical average message processing time as an array. */
      toHistoricalMessageAverageProcessingTimeData: function(metrics, timeResolution, processorId) {
        var processorIds = angular.isNumber(processorId) ?
          [processorId] : this._getProcessorIds();
        return this._getProcessorHistoricalMetrics(processorIds, timeResolution,
          metrics['processTime'], 'mean', d3.mean);
      },

      /**
       * Return the historical average message receive latency of particular processor or the latency on
       * critical path as an array.
       */
      toHistoricalMessageReceiveLatencyData: function(metrics, timeResolution, processorId) {
        if (angular.isNumber(processorId)) {
          return this._getProcessorHistoricalMetrics([processorId], timeResolution,
            metrics['receiveLatency'], 'mean', d3.mean);
        }
        return this.toHistoricalCriticalPathLatencyData(metrics, timeResolution);
      },

      /**
       * Return the historical message receive latency on critical path as an array.
       */
      toHistoricalCriticalPathLatencyData: function(metrics, timeResolution) {
        var that = this;
        var aggregateFn = undefined;
        var unsort = true;
        var processingTimeLookup = this._getProcessorHistoricalMetrics(this.sortedNonSourceProcessorIds,
          timeResolution, metrics['processTime'], 'mean', aggregateFn, unsort);
        var receiveLatencyLookup = this._getProcessorHistoricalMetrics(this.sortedNonSourceProcessorIds,
          timeResolution, metrics['receiveLatency'], 'mean', aggregateFn, unsort);

        var metricTimeArray = _.union(_.keys(processingTimeLookup), _.keys(receiveLatencyLookup));
        var result = {};
        _.forEach(metricTimeArray.sort(), function(time) {
          var latencyLookup = {};
          _.forEach(that.sortedNonSourceProcessorIds, function(processorId) {
            latencyLookup[processorId] =
              ((processingTimeLookup[time] || {})[processorId] || 0) +
              ((receiveLatencyLookup[time] || {})[processorId] || 0);
          });
          _.forEach(that.sortedNonSourceProcessorIds, function(processorId) {
            var predecessorsIds = that.predecessors[processorId];
            if (predecessorsIds.length) {
              var predecessorsLatencies = _.map(predecessorsIds, function(id) {
                return latencyLookup[id] || 0;
              });
              latencyLookup[processorId] += _.max(predecessorsLatencies);
            }
          });
          result[time] = _.max(_.values(latencyLookup));
        });
        return result;
      },

      /** Return particular historical metrics value of processor's tasks as an associative array. */
      _getProcessorHistoricalMetrics: function(processorIds, timeResolution, metrics, field, fn, unsort) {
        var result = {};
        _.forEach(metrics, function(processorMetrics, key) {
          var processorId = Number(key);
          if (_.includes(processorIds, processorId)) {
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
        return unsort ? result : _keysSortedObject(result);
      },

      /** Return the depth of the hierarchy layout */
      hierarchyDepth: function() {
        return _.max(_(this._getActiveProcessors()).pluck('hierarchy').value());
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