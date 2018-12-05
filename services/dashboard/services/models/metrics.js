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

/** TODO: to be absorbed as scalajs (#458) */
  .factory('Metrics', [function () {
    'use strict';

    var decoder = {
      _common: function (data) {
        return {
          meta: decoder._extractPathAndName(data.value.name),
          time: Number(data.time)
        };
      },
      _extractPathAndName: function (name) {
        var tuple = name.split(':');
        return tuple.length === 2 ?
        {path: tuple[0], name: tuple[1]} :
        {path: '', name: name};
      },
      meter: function (data) {
        var result = decoder._common(data);
        var value = data.value;
        result.values = {
          count: Number(value.count),
          meanRate: Number(value.meanRate),
          movingAverage1m: Number(value.m1)
        };
        return result;
      },
      histogram: function (data) {
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
      gauge: function (data) {
        var result = decoder._common(data);
        var value = data.value;
        result.value = Number(value.value);
        return result;
      },
      /** automatically guess metric type and decode or return null */
      $auto: function (data) {
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
      }
    };

    return decoder;
  }])
;
