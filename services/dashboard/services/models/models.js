/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

/** TODO: to be absorbed as scalajs */
  .factory('models', ['restapi', 'locator', 'StreamingDag', 'Metrics',
    function(restapi, locator, StreamingDag, Metrics) {
      'use strict';

      var util = {
        usage: function(current, total) {
          return total > 0 ? 100 * current / total : 0;
        },
        flatten: function(array, fn) {
          var result = {};
          _.forEach(array, function(item) {
            if (fn) {
              item = fn(item);
            }
            result[item[0]] = item[1];
          });
          return result;
        },
        getOrCreate: function(obj, prop, init) {
          if (!obj.hasOwnProperty(prop)) {
            obj[prop] = init;
          }
          return obj[prop];
        }
      };

      /**
       * Retrieves a model from backend as a promise.
       * The resolved object will have two special methods.
       *   `$subscribe` - watch model changes within a scope.
       *   `$data` - return pure model data without these two methods.
       */
      function get(path, decodeFn, args) {
        return restapi.get(path).then(function(response) {
          var oldModel;
          var model = decodeFn(response.data, args);

          model.$subscribe = function(scope, onData) {
            restapi.subscribe(path, scope, function(data) {
              var newModel = decodeFn(data, args);
              if (!_.isEqual(newModel, oldModel)) {
                onData(newModel);
                oldModel = newModel;
              }
            }, (args || {}).period);
          };

          model.$data = function() {
            return _.omit(model, function(value) {
              return _.isFunction(value);
            });
          };

          return model;
        });
      }

      var decoder = {
        _asAssociativeArray: function(objs, decodeFn, keyName) {
          var result = {};
          _.map(objs, function(obj) {
            var model = decodeFn(obj);
            var key = model[keyName];
            result[key] = model;
          });
          return result;
        },
        _location: function(actorPath) {
          return actorPath
            .split('@')[1]
            .split('/')[0];
        },
        _jvm: function(s) {
          var tuple = s.split('@');
          return {
            pid: tuple[0],
            hostname: tuple[1]
          };
        },
        /** Do the necessary deserialization. */
        master: function(wrapper) {
          var obj = wrapper.masterDescription;
          angular.merge(obj, {
            // upickle conversion
            cluster: obj.cluster.map(function(tuple) {
              return tuple.join(':');
            }),
            jvm: decoder._jvm(obj.jvmName),
            leader: obj.leader.join(':'),
            // extra properties/methods
            isHealthy: obj.masterStatus === 'synced',
            configLink: restapi.masterConfigLink()
          });
          return obj;
        },
        workers: function(objs) {
          return decoder._asAssociativeArray(objs, decoder.worker, 'workerId');
        },
        worker: function(obj) {
          var slotsUsed = obj.totalSlots - obj.availableSlots;
          return angular.merge(obj, {
            // extra properties
            jvm: decoder._jvm(obj.jvmName),
            location: decoder._location(obj.actorPath),
            isHealthy: obj.state === 'active',
            slots: {
              usage: util.usage(slotsUsed, obj.totalSlots),
              used: slotsUsed,
              total: obj.totalSlots
            },
            // extra methods
            pageUrl: locator.worker(obj.workerId),
            configLink: restapi.workerConfigLink(obj.workerId)
          });
        },
        apps: function(wrapper) {
          var objs = wrapper.appMasters;
          return decoder._asAssociativeArray(objs, decoder.appSummary, 'appId');
        },
        appSummary: function(obj) {
          // todo: add `type` field to summary and detailed app response
          angular.merge(obj, {
            type: 'streaming'
          });

          return angular.merge(obj, {
            // extra properties
            isRunning: obj.status === 'active',
            location: decoder._location(obj.appMasterPath),
            // extra methods
            pageUrl: locator.app(obj.appId, obj.type),
            terminate: function() {
              return restapi.killApp(obj.appId);
            },
            restart: function() {
              return restapi.restartAppAsync(obj.appId);
            }
          });
        },
        app: function(obj) {
          // todo: add `type` field to summary and detailed app response
          angular.merge(obj, {
            status: 'active',
            type: obj.hasOwnProperty('dag') ? 'streaming' : ''
          });

          // upickle conversion 1: streaming app related decoding
          obj.processors = util.flatten(obj.processors);
          obj.processorLevels = util.flatten(obj.processorLevels);
          if (obj.dag && obj.dag.edgeList) {
            obj.dag.edgeList = util.flatten(obj.dag.edgeList, function(item) {
              return [item[0] + '_' + item[2],
                {source: parseInt(item[0]), target: parseInt(item[2]), type: item[1]}];
            });
          }

          // upickle conversion 2a: convert array to dictionary
          obj.executors = _.object(_.pluck(obj.executors, 'executorId'), obj.executors);

          // upickle conversion 2b: add extra executor properties and methods
          _.forEach(obj.executors, function(executor) {
            angular.merge(executor, {
              isRunning: executor.status === 'active',
              pageUrl: locator.executor(obj.appId, obj.type, executor.executorId),
              workerPageUrl: locator.worker(executor.workerId)
            });
          });

          // upickle conversion 2c: task count is executor specific property for streaming app
          _.forEach(obj.processors, function(processor) {
            var taskCountLookup = util.flatten(processor.taskCount);
            // Backend returns executor ids, but names as `executor`. We change them to real executors.
            processor.executors = _.map(processor.executors, function(executorId) {
              var executor = obj.executors[executorId];
              var processorExecutor = angular.copy(executor); // The task count is for particular processor, so we make a copy
              processorExecutor.taskCount = taskCountLookup[executorId].count;
              // Update global executor task count by the way
              executor.taskCount = (executor.taskCount || 0) + processorExecutor.taskCount;
              return processorExecutor;
            });
          });

          angular.merge(obj, {
            // extra properties
            aliveFor: moment() - obj.startTime,
            isRunning: true, // todo: handle empty response, which is the case application is stopped
            // extra methods
            pageUrl: locator.app(obj.appId, obj.type),
            configLink: restapi.appConfigLink(obj.appId),
            terminate: function() {
              return restapi.killApp(obj.appId);
            }
          });
          return obj;
        },
        appExecutor: function(obj) {
          return angular.merge(obj, {
            // extra properties and methods
            jvm: decoder._jvm(obj.jvmName),
            workerPageUrl: locator.worker(obj.workerId),
            configLink: restapi.appExecutorConfigLink(obj.appId, obj.id)
          });
        },
        appMetricsOld: function(wrapper) {
          return _.map(wrapper.metrics, function(obj) {
            return Metrics.$auto(obj, /*addMeta=*/true);
          });
        },
        appStallingProcessors: function(wrapper) {
          return _.groupBy(wrapper.tasks, 'processorId');
        },
        metrics: function(wrapper, args) {
          var metrics = decoder._metrics(wrapper);
          // Unbox one element array as object
          _.forEach(metrics, function(metricsGroup) {
            _.forEach(metricsGroup, function(timeSortedArray, path) {
              metricsGroup[path] = _.last(timeSortedArray);
            });
          });

          // Reduce 2d array to 1d, if we want to filter particular search path.
          if (args.filterPath) {
            decoder._removeUnrelatedMetricsFrom2dArray(metrics, args.filterPath);
          }
          return metrics;
        },
        /** Decode metrics and desample them by a given period and a given number of points. */
        historicalMetricsDesampled: function(wrapper, args) {
          if (!args.hasOwnProperty('timeStart')) {
            args.timeStart = Number.MIN_VALUE;
          }

          var metrics = decoder._metrics(wrapper);

          // Calculate the range of metrics selection
          var timeMax = args.timeStart;
          _.forEach(metrics, function(metricsGroup) {
            _.forEach(metricsGroup, function(timeSortedArray) {
              timeMax = Math.max(timeMax, _.last(timeSortedArray).time);
            });
          });
          var timeStop = Math.ceil(timeMax / args.period) * args.period;
          if (timeStop > timeMax) {
            timeStop -= args.period;
          }
          var timeStart = Math.max(args.timeStart, timeStop - args.period * args.points);

          _.forEach(metrics, function(metricsGroup) {
            _.forEach(metricsGroup, function(timeSortedArray, path) {
              // Reduce selection range and then desample metrics
              metricsGroup[path] = Metrics.$desample(
                _.filter(timeSortedArray, function(metric) {
                  return metric.time >= timeStart && metric.time <= timeStop;
                }), args.period);
            });
          });

          // The function will be called periodically. To return new metrics only, we need to remember
          // previous fetch start time. As a trick, we store the value into `args`, which can be
          // accessed by the next function call.
          args.timeStart = timeStop;

          // Reduce 2d array to 1d, if we want to filter particular search path.
          if (args.filterPath) {
            decoder._removeUnrelatedMetricsFrom2dArray(metrics, args.filterPath);
          }
          return metrics;
        },
        /**
         * Note that it returns a 2d associative array.
         * The 1st level key is the metric class (e.g. memory.total.used)
         * The snd level key is the object path (e.g. master or app0.processor0)
         * The value is an array of metrics, which are sorted by time.
         */
        _metrics: function(wrapper) {
          var result = {};
          _.forEach(wrapper.metrics, function(obj) {
            var metric = Metrics.$auto(obj);
            if (metric) {
              var metricsGroup = util.getOrCreate(result, metric.meta.clazz, {});
              var metricSeries = util.getOrCreate(metricsGroup, metric.meta.path, {});
              delete metric.meta; // As meta is in the keys, we don't need it in every metric.
              metricSeries[metric.time] = metric;
            }
          });

          // Remove duplicates and sort metrics by time defensively
          // https://github.com/gearpump/gearpump/issues/1385
          _.forEach(result, function(metricsGroup) {
            _.forEach(metricsGroup, function(metricSeries, path) {
              metricsGroup[path] = _.sortBy(metricSeries, 'time');
            });
          });
          return result;
        },
        /** Remove related metrics paths and change the given 2d array to 1d. */
        _removeUnrelatedMetricsFrom2dArray: function(metrics, filterPath) {
          _.forEach(metrics, function(metricsGroup, clazz) {
            if (metricsGroup.hasOwnProperty(filterPath)) {
              metrics[clazz] = metricsGroup[filterPath];
            } else {
              delete metrics[clazz];
            }
          });
        }
      };

      var getter = {
        master: function() {
          return get('/master',
            decoder.master);
        },
        masterMetrics: function() {
          return getter._metrics('/master/metrics/', 'master');
        },
        masterHistoricalMetrics: function(period, points) {
          return getter._historicalMetrics('/master/metrics/', 'master',
            period, points);
        },
        workers: function() {
          return get('/master/workerlist',
            decoder.workers);
        },
        worker: function(workerId) {
          return get('/worker/' + workerId,
            decoder.worker);
        },
        workerMetrics: function(workerId) {
          return getter._metrics('/worker/' + workerId + '/metrics/', 'worker' + workerId);
        },
        workerHistoricalMetrics: function(workerId, period, points) {
          return getter._historicalMetrics('/worker/' + workerId + '/metrics/', 'worker' + workerId,
            period, points);
        },
        apps: function() {
          return get('/master/applist',
            decoder.apps);
        },
        app: function(appId) {
          return get('/appmaster/' + appId + '?detail=true',
            decoder.app);
        },
        /** Note that executor related metrics will be excluded. */
        appMetrics: function(appId) {
          var params = '?readLatest=true';
          return get('/appmaster/' + appId + '/metrics/app' + appId + '.processor*' + params,
            decoder.appMetricsOld);
        },
        appHistoricalMetrics: function(appId, period, points) {
          return get('/appmaster/' + appId + '/metrics/app' + appId + '.processor*',
            decoder.historicalMetricsDesampled, {
              period: period, points: points
            });
        },
        appExecutor: function(appId, executorId) {
          return get('/appmaster/' + appId + '/executor/' + executorId,
            decoder.appExecutor);
        },
        appExecutorMetrics: function(appId, executorId) {
          return getter._metrics(
            '/appmaster/' + appId + '/metrics/', 'app' + appId + '.executor' + executorId);
        },
        appExecutorHistoricalMetrics: function(appId, executorId, period, count) {
          return getter._historicalMetrics(
            '/appmaster/' + appId + '/metrics/', 'app' + appId + '.executor' + executorId,
            period, count);
        },
        appStallingProcessors: function(appId) {
          return get('/appmaster/' + appId + '/stallingtasks',
            decoder.appStallingProcessors);
        },
        _metrics: function(pathPrefix, path) {
          return get(pathPrefix + path + '?readLatest=true',
            decoder.metrics, {filterPath: path});
        },
        _historicalMetrics: function(pathPrefix, path, period, points) {
          return get(pathPrefix + path,
            decoder.historicalMetricsDesampled, {
              period: period, points: points, filterPath: path
            });
        }
      };

      return {
        $get: getter,
        // TODO: scalajs should return a app.details object with dag, if it is a streaming application.
        createDag: function(clock, processors, levels, edges) {
          var dag = new StreamingDag(clock, processors, levels, edges);
          dag.replaceProcessor = restapi.replaceDagProcessor;
          return dag;
        }
      };
    }])
;