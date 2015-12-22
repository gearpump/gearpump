/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

/** TODO: to be absorbed as scalajs */
  .factory('models', ['$timeout', 'conf', 'restapi', 'locator', 'StreamingDag', 'Metrics',
    function($timeout, conf, restapi, locator, StreamingDag, Metrics) {
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
        args = args || {};
        return restapi.get(path).then(function(response) {
          var oldModel;
          var model = decodeFn(response.data, args);

          model.$subscribe = function(scope, onData, onError) {
            restapi.subscribe(args.pathOverride || path, scope, function(data) {
              try {
                var newModel = decodeFn(data, args);
                if (!_.isEqual(newModel, oldModel)) {
                  oldModel = newModel;
                  return onData(newModel);
                }
              } catch (ex) {
                if (angular.isFunction(onError)) {
                  return onError(data);
                }
              }
            }, args.period);
          };

          model.$data = function() {
            return _.omit(model, _.isFunction);
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
        _akkaAddr: function(actorPath) {
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
        partitioners: function(wrapper) {
          return wrapper.partitioners;
        },
        workers: function(objs) {
          return decoder._asAssociativeArray(objs, decoder.worker, 'workerId');
        },
        worker: function(obj) {
          var slotsUsed = obj.totalSlots - obj.availableSlots;
          return angular.merge(obj, {
            // extra properties
            jvm: decoder._jvm(obj.jvmName),
            akkaAddr: decoder._akkaAddr(obj.actorPath),
            isRunning: obj.state === 'active',
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
            akkaAddr: decoder._akkaAddr(obj.appMasterPath),
            // extra methods
            pageUrl: locator.app(obj.appId, obj.type),
            configLink: restapi.appConfigLink(obj.appId),
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

          if (obj.hasOwnProperty('clock') && !moment(Number(obj.clock)).isValid()) {
            console.warn({message: 'invalid application clock', clock: obj.clock});
            delete obj.clock;
          }

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
        /** Return a map. the key is processor id, the value is an array of its stalling tasks */
        appStallingTasks: function(wrapper) {
          var result = _.groupBy(wrapper.tasks, 'processorId');
          _.forEach(result, function(processor, processorId) {
            result[processorId] = _.pluck(processor, 'index');
          });
          return result;
        },
        metrics: function(wrapper, args) {
          var metrics = decoder._metricsGroups(wrapper);
          // Reduce nested array by one level, if we want to filter particular search path.
          if (args.filterPath) {
            decoder._removeUnrelatedMetricsFrom2dArray(metrics, args.filterPath);
          }
          return metrics;
        },
        appMetrics: function(wrapper, args) {
          var metrics = decoder.metrics(wrapper, args);
          return _.mapValues(metrics, function(values) {
            return _.transform(values, function(result, metrics, path) {
              var id = Number(_.last(path.split('.')).replace(/[^0-9]/g, ''));
              result[id] = metrics;
            });
          });
        },
        currentAppProcessorMetrics: function(wrapper, args) {
          var metrics = decoder.metrics(wrapper, args);
          return _.mapValues(metrics, function(values) {
            return _.transform(values, function(result, metrics, path) {
              var taskId = Number(_.last(path.split('.')).replace(/[^0-9]/g, ''));
              result[taskId] = _.last(metrics);
            });
          });
        },
        currentAppTaskMetrics: function(wrapper, args) {
          var metrics = decoder.metrics(wrapper, args);
          return _.mapValues(metrics[args.keyToExtract] || {}, _.last);
        },
        /**
         * Note that it returns a 2d associative array.
         * The 1st level key is the metric class (e.g. memory.total.used)
         * The 2nd level key is the object path (e.g. master or app0.processor0)
         * The value is an array of metrics, which are sorted by time.
         */
        _metricsGroups: function(wrapper) {
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
          return get('master',
            decoder.master);
        },
        masterMetrics: function(updateInterval) {
          return getter._masterMetrics({period: updateInterval});
        },
        masterHistMetrics: function() {
          return getter._masterMetrics({all: true});
        },
        _masterMetrics: function(args) {
          return getter._metrics('master/metrics/', 'master', args);
        },
        partitioners: function() {
          return get('master/partitioners',
            decoder.partitioners);
        },
        workers: function() {
          return get('master/workerlist',
            decoder.workers);
        },
        worker: function(workerId) {
          return get('worker/' + workerId,
            decoder.worker);
        },
        workerMetrics: function(workerId, updateInterval) {
          return getter._workerMetrics(workerId, {period: updateInterval});
        },
        workerHistMetrics: function(workerId) {
          return getter._workerMetrics(workerId, {all: true});
        },
        _workerMetrics: function(workerId, args) {
          return getter._metrics('worker/' + workerId + '/metrics/', 'worker' + workerId, args);
        },
        apps: function() {
          return get('master/applist',
            decoder.apps);
        },
        app: function(appId) {
          return get('appmaster/' + appId + '?detail=true',
            decoder.app);
        },
        /** Note that executor related metrics will be excluded. */
        appMetrics: function(appId, updateInterval) {
          return getter._appMetrics(appId, {period: updateInterval});
        },
        appHistMetrics: function(appId) {
          return getter._appMetrics(appId, {all: true});
        },
        appLatestMetrics: function(appId) {
          return getter._appMetrics(appId, {all: 'latest'});
        },
        _appMetrics: function(appId, args) {
          args.aggregator = 'io.gearpump.streaming.metrics.ProcessorAggregator';
          args.decoder = decoder.appMetrics;
          return getter._metrics('appmaster/' + appId + '/metrics/app' + appId, '', args);
        },
        currentAppProcessorMetrics: function(appId, processorId) {
          var params = '?readOption=readLatest';
          return get('appmaster/' + appId + '/metrics/app' + appId + '.processor' + processorId + params,
            decoder.currentAppProcessorMetrics);
        },
        currentAppTaskMetrics: function(appId, taskClass) {
          var className = _.last(taskClass.split('.')); // meanwhile backend does not support package name
          var classFilter = '*' + className;
          return get('appmaster/' + appId + '/metrics/app' + appId + classFilter + '?readOption=readLatest',
            decoder.currentAppTaskMetrics, {keyToExtract: className});
        },
        appExecutor: function(appId, executorId) {
          return get('appmaster/' + appId + '/executor/' + executorId,
            decoder.appExecutor);
        },
        appExecutorMetrics: function(appId, executorId, updateInterval) {
          return getter._appExecutorMetrics(appId, executorId, {period: updateInterval});
        },
        appExecutorHistMetrics: function(appId, executorId) {
          return getter._appExecutorMetrics(appId, executorId, {all: true});
        },
        _appExecutorMetrics: function(appId, executorId, args) {
          return getter._metrics(
            'appmaster/' + appId + '/metrics/', 'app' + appId + '.executor' + executorId, args);
        },
        appStallingTasks: function(appId) {
          return get('appmaster/' + appId + '/stallingtasks',
            decoder.appStallingTasks);
        },
        _metrics: function(pathPrefix, path, args) {
          args = args || {};
          var aggregatorArg = angular.isString(args.aggregator) ?
            ('&aggregator=' + args.aggregator) : '';
          args.pathOverride = pathPrefix + path + '?readOption=readLatest' + aggregatorArg;
          args.filterPath = path;

          var readOption = args.all === true ? 'readHistory' :
            (args.all === 'latest' ? 'readLatest' : 'readRecent');
          return get(pathPrefix + path + '?readOption=' + readOption + aggregatorArg,
            args.decoder || decoder.metrics, args
          );
        }
      };

      return {
        $get: getter,
        /** Attempts to get model and then subscribe changes as long as the scope is valid. */
        $subscribe: function(scope, getModelFn, onData, period) {
          var shouldCancel = false;
          var promise;
          scope.$on('$destroy', function() {
            shouldCancel = true;
            $timeout.cancel(promise);
          });
          function trySubscribe() {
            if (shouldCancel) {
              return;
            }
            getModelFn().then(function(data) {
              onData(data);
            }, /*onerror=*/function() {
              promise = $timeout(trySubscribe, period || conf.restapiQueryInterval);
            });
          }

          trySubscribe();
        },
        // TODO: scalajs should return a app.details object with dag, if it is a streaming application.
        createDag: function(clock, processors, levels, edges) {
          var dag = new StreamingDag(clock, processors, levels, edges);
          dag.replaceProcessor = restapi.replaceDagProcessor;
          return dag;
        },
        /** Submit a DAG along with jar files */
        submitDag: function(files, dag, onComplete) {
          if (Object.keys(files).length !== 1) {
            return onComplete({success: false, message: 'One jar file is expected'});
          }
          files = _.values(files)[0]; // todo: only one file can be uploaded once (issue 1450)
          return restapi.uploadJars(files, function(response) {
            if (!response.success) {
              return onComplete(response);
            }
            // todo: cannot set jar for individual processor
            angular.forEach(dag.processors, function(elem) {
              elem[1].jar = response.files;
            });
            return restapi.submitDag(dag, function(response) {
              return onComplete(response);
            });
          });
        }
      };
    }])
;