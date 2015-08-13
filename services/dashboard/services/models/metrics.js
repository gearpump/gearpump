/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

/** TODO: to be absorbed as scalajs */
  .factory('Metrics', [function() {
    'use strict';

    function _remainDigits(s) {
      return parseInt(s.replace(/[a-zA-Z]/g, ''));
    }

    function _decodeProcessorName(name) {
      var parts = name.split('.');
      if (parts[0].indexOf('app') !== 0
        || parts[1].indexOf('processor') !== 0
        || parts[2].indexOf('task') !== 0) {
        console.warn('Unexpected name: ' + name);
      }
      return {
        appId: _remainDigits(parts[0]),
        processorId: _remainDigits(parts[1]),
        taskId: _remainDigits(parts[2]),
        clazz: parts[3]
      };
    }

    function _decodeExecutorName(name) {
      var parts = name.split('.');
      if (parts[0].indexOf('app') !== 0
        || (parts[1] !== 'appmaster' && parts[1].indexOf('executor') !== 0)) {
        console.warn('Unexpected name: ' + name);
      }
      return {
        appId: _remainDigits(parts[0]),
        executorId: parts[1] === 'appmaster' ? parts[1] : _remainDigits(parts[1]),
        clazz: parts[2]
      };
    }

    return {
      decode: {
        meter: function(data) {
          // TODO: Serde Meter (#458)
          var value = data.value;
          return {
            meta: _decodeProcessorName(value.name),
            time: Number(data.time),
            values: {
              count: parseInt(value.count),
              meanRate: value.meanRate,
              movingAverage1m: value.m1,
              movingAverage5m: value.m5,
              movingAverage15m: value.m15
            },
            isMeter: true
          };
        },
        histogram: function(data) {
          // TODO: Serde Histogram (#458)
          var value = data.value;
          return {
            meta: _decodeProcessorName(value.name),
            time: Number(data.time),
            values: {
              count: parseInt(value.count),
              minimum: parseInt(value.min),
              maximum: parseInt(value.max),
              mean: value.mean,
              stddev: value.stddev,
              median: value.median,
              p75: value.p75,
              p95: value.p95,
              p98: value.p98,
              p99: value.p99,
              p999: value.p999
            },
            isHistogram: true
          };
        },
        gauge: function(data) {
          // TODO: Serde Gauge (#458)
          var value = data.value;
          return {
            meta: _decodeExecutorName(value.name),
            time: Number(data.time),
            value: Number(value.value),
            isGauge: true
          };
        }
      }
    };
  }])
;