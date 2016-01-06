/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

/** TODO: to be absorbed as scalajs (#458) */
  .factory('Metrics', [function() {
    'use strict';

    var decoder = {
      _common: function(data) {
        return {
          meta: decoder.$name(data.value.name),
          time: Number(data.time)
        };
      },
      meter: function(data) {
        var result = decoder._common(data);
        var value = data.value;
        result.values = {
          count: Number(value.count),
          meanRate: Number(value.meanRate),
          movingAverage1m: Number(value.m1)
        };

        return result;
      },
      histogram: function(data) {
        var result = decoder._common(data);
        var value = data.value;
        result.values = {
          mean: Number(value.mean),
          stddev: Number(value.stddev),
          median: Number(value.median),
          p95: Number(value.p95),
          p99: Number(value.p99),
          p999: Number(value.p999)
        };

        return result;
      },
      gauge: function(data) {
        var result = decoder._common(data);
        var value = data.value;
        result.value = Number(value.value);

        return result;
      },
      /** automatically guess metric type and decode or return null */
      $auto: function(data) {
        switch (data.value.$type) {
          case 'io.gearpump.metrics.Metrics.Meter':
            return decoder.meter(data);
          case 'io.gearpump.metrics.Metrics.Histogram':
            return decoder.histogram(data);
          case 'io.gearpump.metrics.Metrics.Gauge':
            return decoder.gauge(data);
          default:
            console.warn({message: 'Unknown metric type', type: data.value.$type});
            return;
        }
      },
      /** Decode name string as object path and metric class */
      $name: function(name) {
        var tuple = name.split(':');
        return tuple.length === 2 ?
        {path: tuple[0], clazz: tuple[1]} :
        {path: '', clazz: name};
      }
    };

    return decoder;
  }])
;