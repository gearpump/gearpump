/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

/** TODO: to be absorbed as scalajs (#458) */
  .factory('Metrics', [function() {
    'use strict';

    function _remainDigits(s) {
      return parseInt(s.replace(/[^0-9]/g, ''));
    }

    function _decodeProcessorName(path) {
      var parts = path.split('.');
      if (parts.length !== 3
        || parts[0].indexOf('app') !== 0
        || parts[1].indexOf('processor') !== 0
        || parts[2].indexOf('task') !== 0) {
        console.error('Unexpected path: ' + path);
        parts = [-1, -1, -1];
      }
      return {
        appId: _remainDigits(parts[0]),
        processorId: _remainDigits(parts[1]),
        taskId: _remainDigits(parts[2])
      };
    }

    function _decodeExecutorName(path) {
      var parts = path.split('.');
      if (parts.length !== 2
        || parts[0].indexOf('app') !== 0
        || (parts[1] !== 'appmaster' && parts[1].indexOf('executor') !== 0)) {
        console.error('Unexpected path: ' + path);
        parts = [-1, -1];
      }
      return {
        appId: _remainDigits(parts[0]),
        executorId: parts[1] === 'appmaster' ? parts[1] : _remainDigits(parts[1])
      };
    }

    function _createAveragedMetricsFromArray(array) {
      if (!array.length) {
        console.error('Array should contain at least one element.');
        return;
      }

      var result = angular.copy(array[0]);
      if (array.length > 1) {
        if (result.hasOwnProperty('values')) {
          _.forEach(result.values, function(field) {
            result.values[field] = d3.mean(array, function(metric) {
              return metric.values[field];
            });
          });
        } else if (result.hasOwnProperty('value')) {
          result.value = d3.mean(array, function(metric) {
            return metric.value;
          });
        }
      }
      return result;
    }

    function toPrecision2(any) {
      return _.floor(any, 2);
    }

    var decoder = {
      _common: function(data) {
        return {
          meta: decoder.$name(data.value.name),
          time: Number(data.time)
        };
      },
      meter: function(data, addMeta) {
        var result = decoder._common(data);
        var value = data.value;
        result.values = {
          count: Number(value.count),
          meanRate: toPrecision2(value.meanRate),
          movingAverage1m: toPrecision2(value.m1)
        };

        if (addMeta) {
          result.isMeter = true;
          angular.merge(result.meta, _decodeProcessorName(result.meta.path));
        }

        return result;
      },
      histogram: function(data, addMeta) {
        var result = decoder._common(data);
        var value = data.value;
        result.values = {
          mean: toPrecision2(value.mean),
          stddev: toPrecision2(value.stddev),
          median: toPrecision2(value.median),
          p95: toPrecision2(value.p95),
          p99: toPrecision2(value.p99),
          p999: toPrecision2(value.p999)
        };

        if (addMeta) {
          result.isHistogram = true;
          angular.merge(result.meta, _decodeProcessorName(result.meta.path));
        }

        return result;
      },
      gauge: function(data, addMeta) {
        var result = decoder._common(data);
        var value = data.value;
        result.value = toPrecision2(value.value);

        if (addMeta) {
          result.isGauge = true;
          angular.merge(result.meta, _decodeExecutorName(result.meta.path));
        }

        return result;
      },
      /** automatically guess metric type and decode or return null */
      $auto: function(data, addMeta) {
        switch (data.value.$type) {
          case 'io.gearpump.metrics.Metrics.Meter':
            return decoder.meter(data, addMeta);
          case 'io.gearpump.metrics.Metrics.Histogram':
            return decoder.histogram(data, addMeta);
          case 'io.gearpump.metrics.Metrics.Gauge':
            return decoder.gauge(data, addMeta);
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