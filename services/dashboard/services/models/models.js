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
        extractLocation: function(actorPath) {
          return actorPath
            .split('@')[1]
            .split('/')[0];
        },
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
        }
      };

      /**
       * Retrieves a model from backend as a promise. The resolved object will have a $subscribe method, which can be
       * used to watch model changes within a scope. */
      function get(path, decodeFn, subscribePath) {
        return restapi.get(path).then(function(response) {
          var oldModel;
          var model = decodeFn(response.data);

          model.$subscribe = function(scope, onData) {
            restapi.subscribe(subscribePath || path, scope, function(data) {
              var newModel = decodeFn(data);
              if (!_.isEqual(newModel, oldModel)) {
                onData(newModel);
                oldModel = newModel;
              }
            });
          };

          model.$data = function() {
            var result = {};
            _.forEach(model, function(value, key) {
              if (!angular.isFunction(value)) {
                result[key] = value;
              }
            });
            return result;
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
            location: util.extractLocation(obj.actorPath),
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
            location: util.extractLocation(obj.appMasterPath),
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
          // upickle conversion 2a: task count is executor specific property for streaming app
          _.forEach(obj.processors, function(processor) {
            var taskCountLookup = util.flatten(processor.taskCount);
            processor.executors = _.map(processor.executors, function(executorId) {
              var executor = obj.executors[executorId];
              executor.taskCount = executorId in taskCountLookup ?
                taskCountLookup[executorId].count : 0;
              return executor;
            });
          });

          // upickle conversion 2b: add extra executor properties and methods
          _.forEach(obj.executors, function(executor) {
            angular.merge(executor, {
              isRunning: executor.status === 'active',
              pageUrl: locator.executor(obj.appId, obj.type, executor.executorId),
              workerPageUrl: locator.worker(executor.workerId)
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
            return Metrics.$auto(obj);
          });
        },
        appStallingProcessors: function(wrapper) {
          return _.groupBy(wrapper.tasks, 'processorId');
        },
        metrics: function(wrapper) {
          var result = {};
          _.forEach(wrapper.metrics, function(metric) {
            var data = Metrics.$auto(metric, /*noMeta=*/true);
            if (data) {
              var metricName = Metrics.$name(metric.value.name).name;
              if (!result.hasOwnProperty(metricName)) {
                result[metricName] = [];
              }
              result[metricName].push(data);
            }
          });
          return result;
        }
      };

      return {
        $get: {
          master: function() {
            return get('/master',
              decoder.master);
          },
          masterMetrics: function(all) {
            var base = '/master/metrics/master';
            var firstTimePath = base + (all ? '' : '?readLatest=true');
            var subscribePath = base + '?readLatest=true';
            return get(firstTimePath, decoder.metrics, subscribePath);
          },
          workers: function() {
            return get('/master/workerlist',
              decoder.workers);
          },
          worker: function(workerId) {
            return get('/worker/' + workerId,
              decoder.worker);
          },
          workerMetrics: function(workerId, all) {
            var base = '/worker/' + workerId + '/metrics/worker' + workerId;
            var firstTimePath = base + (all ? '' : '?readLatest=true');
            var subscribePath = base + '?readLatest=true';
            return get(firstTimePath, decoder.metrics, subscribePath);
          },
          apps: function() {
            return get('/master/applist',
              decoder.apps);
          },
          app: function(appId) {
            return get('/appmaster/' + appId + '?detail=true',
              decoder.app);
          },
          appMetrics: function(appId, all) {
            var base = '/appmaster/' + appId + '/metrics/app' + appId;
            var firstTimePath = base + (all ? '' : '?readLatest=true');
            var subscribePath = base + '?readLatest=true';
            return get(firstTimePath, decoder.appMetricsOld, subscribePath);
          },
          appExecutor: function(appId, executorId) {
            return get('/appmaster/' + appId + '/executor/' + executorId,
              decoder.appExecutor);
          },
          appExecutorMetrics: function(appId, executorId, all) {
            var base = '/appmaster/' + appId + '/metrics/app' + appId + '.executor' + executorId;
            var firstTimePath = base + (all ? '' : '?readLatest=true');
            var subscribePath = base + '?readLatest=true';
            return get(firstTimePath, decoder.metrics, subscribePath);
          },
          appStallingProcessors: function(appId) {
            return get('/appmaster/' + appId + '/stallingtasks',
              decoder.appStallingProcessors);
          }
        },

        // TODO: scalajs should return a app.details object with dag, if it is a streaming application.
        createDag: function(appId, clock, processors, levels, edges, executors) {
          var dag = new StreamingDag(appId, clock, processors, levels, edges, executors);
          dag.replaceProcessor = restapi.replaceDagProcessor;
          return dag;
        }
      };
    }])
;