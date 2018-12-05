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

  .service('StreamingAppMetricsProvider', ['MetricsProvider', function (MetricsProvider) {
    'use strict';

    /** This repository stores streaming app related metrics. */
    function StreamingAppMetricsProvider(ids) {
      this.impl = new MetricsProvider(ids);
    }

    StreamingAppMetricsProvider.prototype = {

      /** Add metrics or update to the latest value and return the number of updated metrics. */
      updateLatestMetrics: function (metrics) {
        return this.impl.update(metrics, {latestOnly: true});
      },

      /**
       * Add all metrics to a multiple dimension associative array and return the number of updated metrics.
       * Note that every metric will be stored only once and the metric time is the closest retain interval.
       */
      updateAllMetricsByRetainInterval: function (metrics, timeResolution) {
        return this.impl.update(metrics, {latestOnly: false, timeResolution: timeResolution});
      },

      /** Return all metric time as an array in ascending order. */
      getMetricTimeArray: function () {
        return this.impl.getMetricTimeArray();
      },

      /** Return total and moving average received messages of one or more processors */
      getReceiveMessageTotalAndRate: function (ids, time) {
        return this._getMeterMetricsTotalAndRate(ids, 'receiveThroughput', time);
      },

      /** Return total and moving average sent messages of one or more processors */
      getSendMessageTotalAndRate: function (ids, time) {
        return this._getMeterMetricsTotalAndRate(ids, 'sendThroughput', time);
      },

      _getMeterMetricsTotalAndRate: function (ids, name, time) {
        var result = this.impl.getMeterMetricSumByFields(ids, name, ['count', 'movingAverage1m'], time);
        return {
          total: result.count,
          rate: result.movingAverage1m
        };
      },

      /** Return moving average received messages of one or more processors */
      getReceiveMessageMovingAverage: function (ids, time) {
        var field = 'movingAverage1m';
        return this.impl.getMeterMetricSumByFields(ids, 'receiveThroughput', [field], time)[field];
      },

      /** Return moving average sent messages of one or more processors */
      getSendMessageMovingAverage: function (ids, time) {
        var field = 'movingAverage1m';
        return this.impl.getMeterMetricSumByFields(ids, 'sendThroughput', [field], time)[field];
      },

      /** Return the average message processing time of one or more processors */
      getAverageMessageProcessingTime: function (ids, time) {
        var fallback = 0;
        return this.impl.getHistogramMetricAverageOrElse(ids, 'processTime', 'mean', fallback, time);
      },

      /** Return the average message receive latency of one or more processors */
      getAverageMessageReceiveLatency: function (ids, time) {
        var fallback = 0;
        return this.impl.getHistogramMetricAverageOrElse(ids, 'receiveLatency', 'mean', fallback, time);
      },

      /**
       * Return an array of message receive throughput of one or more processors, which is
       * aggregated by retain interval.
       */
      getReceiveMessageThroughputByRetainInterval: function (ids) {
        return this.impl.getAggregatedMetricsByRetainInterval(ids,
          'receiveThroughput', 'movingAverage1m', {aggregateFn: _.sum});
      },

      /**
       * Return an array of message send throughput of one or more processors, which is aggregated
       * by retain interval.
       */
      getSendMessageThroughputByRetainInterval: function (ids) {
        return this.impl.getAggregatedMetricsByRetainInterval(ids,
          'sendThroughput', 'movingAverage1m', {aggregateFn: _.sum});
      },

      /**
       * Return an array of the average message processing time of one or more processors, which is aggregated
       * by retain interval.
       */
      getAverageMessageProcessingTimeByRetainInterval: function (ids) {
        return this.impl.getAggregatedMetricsByRetainInterval(ids,
          'processTime', 'mean', {aggregateFn: d3.mean});
      },

      /**
       * Return an array of the average message receive latency of one or more processors, which is aggregated
       * by retain interval.
       */
      getAverageMessageReceiveLatencyByRetainInterval: function (ids) {
        return this.impl.getAggregatedMetricsByRetainInterval(ids,
          'receiveLatency', 'mean', {aggregateFn: d3.mean});
      },

      /** Return an array of processor metrics of particular metric name. */
      getMetricsByMetricName: function (name) {
        return _.mapValues(this.impl.data, function (metricsGroups) {
          return metricsGroups[name].latest;
        });
      }

    };

    return StreamingAppMetricsProvider;
  }])
;
