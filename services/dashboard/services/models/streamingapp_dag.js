/*
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
angular.module('io.gearpump.models')

  .service('StreamingAppDag', ['Dag', 'StreamingAppMetricsProvider', function (Dag, StreamingAppMetricsProvider) {
    'use strict';

    /**
     * The class represents a streaming application. It has an application clock, which always goes forwards.
     * From topology perspective, a streaming application consists of a set of processors and edges, which
     * builds up a DAG. Every processor and edge has a set of metrics. The metrics will be updated periodically
     * at run time. The topology of the DAG can be changed at runtime as well.
     */
    function StreamingAppDag(clock, processors, edges) {
      this.metricsUpdateTime = 0;
      this.stallingTasks = {};
      this.setData(clock, processors, edges);
    }

    StreamingAppDag.prototype = {

      /** Set the current dag data */
      setData: function (clock, processors, edges) {
        this.clock = clock;
        // The physical view contains all the processors of the application, even those dead processors.
        // The logical view contains only the alive processors. Once processors or edges are changed, we
        // will re-create both dag objects for consistency.
        if (!this.dagPhysicalView || !this.dagPhysicalView.isEqual(processors, edges)) {
          this.dagPhysicalView = new Dag(processors, edges);
          this.metricsProvider = new StreamingAppMetricsProvider(this.dagPhysicalView.getProcessorIds());

          var activeProcessors = filterActiveProcessors(processors);
          var activeEdges = filterRelatedEdges(edges, activeProcessors);
          this.dagLogicalView = new Dag(activeProcessors, activeEdges);
        }
      },

      setStallingTasks: function (tasks) {
        this.stallingTasks = tasks;
      },

      /** Update (or add) a set of latest metrics. */
      updateLatestMetrics: function (metrics) {
        var count = this.metricsProvider.updateLatestMetrics(metrics);
        if (count > 0) {
          // advance the metric update time so that view can subscribe metric changes
          this.metricsUpdateTime = _.last(this.metricsProvider.getMetricTimeArray());
        }
      },

      /** Replace existing historical metrics with new data. */
      replaceHistoricalMetrics: function (metrics, timeResolution) {
        // as the metrics can be application wide or processor wide, to be perfectly clean, we store
        // historical metrics in a separate metrics provider
        var processorIds = this.dagPhysicalView.getProcessorIds();
        this.histMetricsProvider = new StreamingAppMetricsProvider(processorIds);
        this.histMetricsProvider.updateAllMetricsByRetainInterval(metrics, timeResolution);
      },

      /** Return the number of current active processors. */
      getProcessorCount: function () {
        return this.dagLogicalView.getProcessorIds().length;
      },

      /** Return the processor object. */
      getProcessor: function (processorId) {
        return this.dagPhysicalView.processors[processorId];
      },

      /** Return the logical view of dag with weighted information for drawing a DAG graph. */
      getWeightedDagView: function () {
        var viewProvider = this.dagLogicalView;
        var processors = viewProvider.processors;
        var edges = viewProvider.edges;

        var weights = {};
        _.forEach(processors, function (processor) {
          weights[processor.id] = this._calculateProcessorWeight(processor.id);
        }, this);

        var bandwidths = {};
        _.forEach(edges, function (edge, key) {
          bandwidths[key] = this._calculateEdgeBandwidth(edge);
        }, this);

        return {
          processors: processors,
          processorStallingTasks: this.stallingTasks,
          processorWeights: weights,
          edges: edges,
          edgeBandwidths: bandwidths
        };
      },

      /** Weight of a processor equals the sum of its send throughput and receive throughput. */
      _calculateProcessorWeight: function (processorId) {
        return Math.max(
          this.metricsProvider.getSendMessageMovingAverage([processorId]),
          this.metricsProvider.getReceiveMessageMovingAverage([processorId])
        );
      },

      /** Bandwidth of an edge equals the minimum of average send throughput and average receive throughput. */
      _calculateEdgeBandwidth: function (edge) {
        var sourceOutdegree = this.dagLogicalView.degrees[edge.from].outdegree;
        var targetIndegree = this.dagLogicalView.degrees[edge.to].indegree;
        var sourceSendThroughput = this.metricsProvider.getSendMessageMovingAverage([edge.from]);
        var targetReceiveThroughput = this.metricsProvider.getReceiveMessageMovingAverage([edge.to]);
        return Math.min(
          sourceOutdegree === 0 ? 0 : Math.round(sourceSendThroughput / sourceOutdegree),
          targetIndegree === 0 ? 0 : Math.round(targetReceiveThroughput / targetIndegree)
        );
      },

      /** Return total received messages and rate of all active sink processors. */
      getSinkProcessorReceivedMessageTotalAndRate: function () {
        var processorIds = this.dagLogicalView.getSinkProcessorIds();
        return this.metricsProvider.getReceiveMessageTotalAndRate(processorIds);
      },

      /** Return total received messages and rate of particular processor. */
      getProcessorReceivedMessages: function (processorId) {
        return this.metricsProvider.getReceiveMessageTotalAndRate([processorId]);
      },

      /** Return total sent messages and rate of all active source processors. */
      getSourceProcessorSentMessageTotalAndRate: function () {
        var processorIds = this.dagLogicalView.getSourceProcessorIds();
        return this.metricsProvider.getSendMessageTotalAndRate(processorIds);
      },

      /** Return total sent messages and rate of particular processor. */
      getProcessorSentMessages: function (processorId) {
        return this.metricsProvider.getSendMessageTotalAndRate([processorId]);
      },

      /** Return the latency on critical path. */
      getCriticalPathLatency: function () {
        var criticalPathAndLatency = this.dagLogicalView.calculateCriticalPathAndLatency(this.metricsProvider);
        return criticalPathAndLatency.latency;
      },

      /** Return the average message processing time of particular processor. */
      getProcessorAverageMessageProcessingTime: function (processorId) {
        return this.metricsProvider.getAverageMessageProcessingTime([processorId]);
      },

      /** Return the average message receive latency of particular processor or the latency on critical path. */
      getProcessorMessageReceiveLatency: function (processorId) {
        return this.metricsProvider.getAverageMessageReceiveLatency([processorId]);
      },

      /** Return the historical message receive throughput of data sink processors as an array. */
      getSinkProcessorHistoricalMessageReceiveThroughput: function () {
        var processorIds = this.dagPhysicalView.getSinkProcessorIds();
        return this.histMetricsProvider.getReceiveMessageThroughputByRetainInterval(processorIds);
      },

      /** Return the historical message receive throughput of particular processor as an array. */
      getProcessorHistoricalMessageReceiveThroughput: function (processorId) {
        return this.histMetricsProvider.getReceiveMessageThroughputByRetainInterval([processorId]);
      },

      /** Return the historical message send throughput of data sink processors as an array. */
      getSourceProcessorHistoricalMessageSendThroughput: function () {
        var processorIds = this.dagPhysicalView.getSourceProcessorIds();
        return this.histMetricsProvider.getSendMessageThroughputByRetainInterval(processorIds);
      },

      /** Return the historical message send throughput of particular processor as an array. */
      getProcessorHistoricalMessageSendThroughput: function (processorId) {
        return this.histMetricsProvider.getSendMessageThroughputByRetainInterval([processorId]);
      },

      /** Return the historical message receive latency on critical path as an array. */
      getHistoricalCriticalPathLatency: function () {
        var provider = this.histMetricsProvider;
        var metricTimeArray = provider.getMetricTimeArray();
        var result = {};
        _.forEach(metricTimeArray, function (time) {
          var criticalPathAndLatency = this.dagPhysicalView.calculateCriticalPathAndLatency(provider, time);
          result[time] = criticalPathAndLatency.latency;
        }, this);
        return result;
      },

      /** Return the historical average message processing time of particular processor as an array. */
      getProcessorHistoricalAverageMessageProcessingTime: function (processorId) {
        return this.histMetricsProvider.getAverageMessageProcessingTimeByRetainInterval([processorId]);
      },

      /** Return the historical average message receive latency of particular processor. */
      getProcessorHistoricalAverageMessageReceiveLatency: function (processorId) {
        return this.histMetricsProvider.getAverageMessageReceiveLatencyByRetainInterval([processorId]);
      },

      /** Return an array of all processors' metrics of particular metric name. */
      getMetricsByMetricName: function (name) {
        return this.metricsProvider.getMetricsByMetricName(name);
      },

      /** Return the logical indegree and outdegree of a processor. */
      getProcessorIndegreeAndOutdegree: function (processorId) {
        return this.dagLogicalView.degrees[processorId];
      },

      /** Return the number of processors on the longest alive path. */
      hierarchyDepth: function () {
        return this.dagLogicalView.hierarchyDepth();
      }

    };

    /** static method */
    function filterActiveProcessors(processors) {
      return _.pick(processors, function (processor) {
        return processor.active;
      });
    }

    /** static method */
    function filterRelatedEdges(edges, processors) {
      return _.pick(edges, function (edge) {
        return processors.hasOwnProperty(edge.from) &&
          processors.hasOwnProperty(edge.to);
      });
    }

    return StreamingAppDag;
  }])
;
