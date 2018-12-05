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

  .service('MetricsProvider', function () {
    'use strict';

    /**
     * The repository stores metrics as a multiple dimension associative array and provides method to access
     * particular metric fields.
     */
    function MetricsProvider(paths) {
      this.paths = _.zipObject(paths, paths); // {map} key is path, value is path (e.g. app1/processor1)
      this.timeLookup = {}; // {map} key is metric time, value is metric time
      this.data = {}; // {multi-dimension associative array} path -> name -> time -> array of metric values
      this.LATEST_METRIC_TIME = 'latest';
    }

    MetricsProvider.prototype = {

      /** Add or update metrics to the repository and return the number of updated metrics. */
      update: function (metrics, args) {
        var that = this;
        var count = 0;
        // metrics is {multi-dimension associative array} name -> path -> array of metric
        _.forEach(metrics, function (pathAndMetrics, name) {
          _.forEach(pathAndMetrics, function (metricArray, path) {
            if (that.paths.hasOwnProperty(path)) {
              count += args.latestOnly ?
                that._updateLatestMetrics(path, name, metricArray) :
                that._updateMetricsByRetainInterval(path, name, metricArray, args.timeResolution);
            }
          });
        });
        return count;
      },

      _updateLatestMetrics: function (path, name, metrics) {
        if (metrics.length > 0) {
          var metric = _.last(metrics);
          this._updateMetric(path, name, this.LATEST_METRIC_TIME, metric.values);
          this.timeLookup[this.LATEST_METRIC_TIME] =
            Math.max(this.timeLookup[this.LATEST_METRIC_TIME] || 0, metric.time);
          return 1;
        }
        return 0;
      },

      _updateMetricsByRetainInterval: function (path, name, metrics, timeResolution) {
        var count = 0;
        if (timeResolution > 0) {
          _.forEach(metrics, function (metric) {
            var retainIntervalTime = Math.floor(metric.time / timeResolution) * timeResolution;
            this._updateMetric(path, name, retainIntervalTime, metric.values);
            count++;
          }, this);
        } else {
          console.warn('Failed to update metrics, because the timeResolution is invalid');
        }
        return count;
      },

      _updateMetric: function (path, name, time, values) {
        this.data[path] = this.data[path] || {};
        this.data[path][name] = this.data[path][name] || {};
        this.data[path][name][time] = values;

        if (time > 0) {
          this.timeLookup[time] = time;
        }
      },

      /** Return all metric time as an array in ascending order. */
      getMetricTimeArray: function () {
        return _.values(this.timeLookup).sort();
      },

      _getMetricFieldOrElse: function (path, name, field, fallback, time) {
        try {
          return time > 0 ?
            this.data[path][name][time][field] :
            this.data[path][name][this.LATEST_METRIC_TIME][field];
        } catch (ex) {
          return fallback;
        }
      },

      /** Return the sum of particular metric field of particular processors. */
      getMeterMetricSum: function (paths, name, field, time) {
        var result = this.getMeterMetricSumByFields(paths, name, [field], time);
        return result[field];
      },

      /** Return a map of the sum of particular metric fields of particular processors. */
      getMeterMetricSumByFields: function (paths, name, fields, time) {
        var result = _.zipObject(fields, _.times(fields.length, 0));
        var that = this;
        _.forEach(paths, function (path) {
          _.forEach(fields, function (field) {
            result[field] += that._getMetricFieldOrElse(path, name, field, 0, time);
          });
        });
        return result;
      },

      /**
       * Return the average of particular metric field of particular processors.
       * Return a fallback value, if no metric value is captured.
       */
      getHistogramMetricAverageOrElse: function (paths, name, field, fallback, time) {
        var array = [];
        _.forEach(paths, function (path) {
          var value = this._getMetricFieldOrElse(path, name, field, false, time);
          if (value !== false) {
            array.push(value);
          }
        }, this);
        return util.mean(array) || fallback;
      },

      /**
       * Batch read particular metric field from the repository and then return a map. The key is metric
       * time; the value is an array of metric field values or the aggregated field value.
       */
      getAggregatedMetricsByRetainInterval: function (paths, name, field, options) {
        var that = this;
        var result = {};

        _.forEach(paths, function (path) {
          var metrics = that._filterMetricsByPathAndName(path, name);
          _.forEach(metrics, function (metric, time) {
            if (time !== that.LATEST_METRIC_TIME) {
              result[time] = result[time] || [];
              if (metric.hasOwnProperty(field)) {
                result[time].push(metric[field]);
              }
            }
          });
        });

        _.forEach(result, function (array, time) {
          result[time] = _.isFunction(options.aggregateFn) ?
            options.aggregateFn(array) : array;
        });
        return options.unsort ? result : util.keysSortedObject(result);
      },

      _filterMetricsByPathAndName: function (path, name) {
        var match = this.data[path];
        if (match && match.hasOwnProperty(name)) {
          return match[name];
        }
      }

    };

    var util = {
      mean: function (array) {
        // No idea why there is no Math.mean() or Array.mean() or _.mean()
        return array.length ? _.sum(array) / array.length : NaN;
      },
      keysSortedObject: function (object) {
        var result = {};
        _.forEach(_.keys(object).sort(), function (key) {
          result[key] = object[key];
        });
        return result;
      }
    };

    return MetricsProvider;
  })
;
