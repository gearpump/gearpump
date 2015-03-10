/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('dashboard.metrics', [])

  .factory('Metrics', [function () {
    return {
      meter: function (data) {
        return {
          name: _decodeName(data.name),
          values: {
            count: parseInt(data.count), // TODO: Integer should a number not a string
            meanRate: data.meanRate,
            movingAverage1m: data.m1,
            movingAverage5m: data.m5,
            movingAverage15m: data.m15
          }
        };
      },

      histogram: function (data) {
        return {
          name: _decodeName(data.name),
          values: {
            count: parseInt(data.count), // TODO: Integer should a number not a string
            minimum: parseInt(data.min), // TODO: Integer should a number not a string
            maximum: parseInt(data.max), // TODO: Integer should a number not a string
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