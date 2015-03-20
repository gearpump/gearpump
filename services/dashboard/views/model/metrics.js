/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('dashboard.metrics', [])

  .factory('Metrics', [function () {
    return {
      decodeMeter: function (data) {
        // TODO: Serde Meter (#458)
        return {
          name: _decodeName(data.name),
          values: {
            count: parseInt(data.count),
            meanRate: data.meanRate,
            movingAverage1m: data.m1,
            movingAverage5m: data.m5,
            movingAverage15m: data.m15
          }
        };
      },

      decodeHistogram: function (data) {
        // TODO: Serde Histogram (#458)
        return {
          name: _decodeName(data.name),
          values: {
            count: parseInt(data.count),
            minimum: parseInt(data.min),
            maximum: parseInt(data.max),
            mean: data.mean,
            stddev: data.stddev,
            median: data.median,
            p75: data.p75,
            p95: data.p95,
            p98: data.p98,
            p99: data.p99,
            p999: data.p999
          }
        };
      }
    };

    function _decodeName(name) {
      var parts = name.split('.');
      var appId = parts[0].substring('app'.length);
      var processorId = parts[1].substring('processor'.length);
      var taskId = parts[2].substring('task'.length);
      var metric = parts[3];
      return {
        appId: parseInt(appId),
        processorId: parseInt(processorId),
        taskId: parseInt(taskId),
        metric: metric
      };
    }
  }])
;