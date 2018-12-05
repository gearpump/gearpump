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
angular.module('dashboard')

/** TODO: refactoring work required */
  .factory('restapi', ['$q', '$http', '$timeout', '$modal', 'Upload', 'conf', 'HealthCheckService',
    function ($q, $http, $timeout, $modal, Upload, conf, HealthCheckService) {
      'use strict';

      function decodeSuccessResponse(data) {
        return angular.merge({
          success: true
        }, (data || {}));
      }

      function decodeErrorResponse(data) {
        var errorMessage = '';
        var stackTrace = [];
        var lines = (data || '').split('\n');
        if (lines.length) {
          errorMessage = lines[0].replace(', error summary:', '');
          stackTrace = lines.slice(1);
        }
        return {success: false, error: errorMessage, stackTrace: stackTrace};
      }

      var restapiV1Root = conf.restapiRoot + 'api/' + conf.restapiProtocol + '/';
      var self = {
        /**
         * Retrieve data from rest service endpoint (HTTP GET) periodically in an angular scope.
         */
        subscribe: function (path, scope, onData, interval) {
          var timeoutPromise;
          var shouldCancel = false;
          scope.$on('$destroy', function () {
            shouldCancel = true;
            $timeout.cancel(timeoutPromise);
          });

          interval = interval || conf.restapiQueryInterval;
          var fn = function () {
            var promise = self.get(path);
            promise.then(function (response) {
              if (!shouldCancel && angular.isFunction(onData)) {
                shouldCancel = onData(response.data);
              }
            }, function (response) {
              if (!shouldCancel && angular.isFunction(onData)) {
                shouldCancel = onData(response.data);
              }
            })
              .finally(function () {
                if (!shouldCancel) {
                  timeoutPromise = $timeout(fn, interval);
                }
              });
          };
          timeoutPromise = $timeout(fn, interval);
        },

        /**
         * Query model from service endpoint and return a promise.
         * Note that if operation is failed, it will return the failure after a default timeout. If
         * health check indicates the service is unavailable, no request will be sent to server, just
         * simple return a failure after a default timeout.
         */
        get: function (path) {
          if (!HealthCheckService.isServiceAvailable()) {
            var deferred = $q.defer();
            _.delay(deferred.reject, conf.restapiQueryTimeout);
            return deferred.promise;
          }
          return $http.get(restapiV1Root + path, {timeout: conf.restapiQueryTimeout});
        },

        /** Get data from server periodically until an user cancellation or scope exit. */
        repeatUntil: function (url, scope, onData) {
          // TODO: Once `subscribe` is turned to websocket push model, there is no need to have this method
          this.subscribe(url, scope,
            function (data) {
              return !onData || onData(data);
            });
        },

        /** Kill a running application */
        killApp: function (appId) {
          var url = restapiV1Root + 'appmaster/' + appId;
          return $http.delete(url);
        },

        /** Restart a running application and return a promise */
        restartAppAsync: function (appId) {
          var url = restapiV1Root + 'appmaster/' + appId + '/restart';
          return $http.post(url);
        },

        /** Return the config link of an application */
        appConfigLink: function (appId) {
          return restapiV1Root + 'appmaster/' + appId + '/config';
        },

        /** Return the config link of an application */
        appExecutorConfigLink: function (appId, executorId) {
          return restapiV1Root + 'appmaster/' + appId + '/executor/' + executorId + '/config';
        },

        /** Return the config link of a worker */
        workerConfigLink: function (workerId) {
          return restapiV1Root + 'worker/' + workerId + '/config';
        },

        /** Return the config link of the master */
        masterConfigLink: function () {
          return restapiV1Root + 'master/config';
        },

        /** Submit an user defined application with user configuration */
        submitUserApp: function (files, fileFieldNames, executorNum, args, onComplete) {
          return self._submitApp(restapiV1Root + 'master/submitapp',
            files, fileFieldNames, executorNum, args, onComplete);
        },

        /** Submit a Storm application */
        submitStormApp: function (files, formFormNames, executorNum, args, onComplete) {
          return self._submitApp(restapiV1Root + 'master/submitstormapp',
            files, formFormNames, executorNum, args, onComplete);
        },

        _submitApp: function (url, files, fileFieldNames, executorNum, args, onComplete) {
          var upload = Upload.upload({
            url: url,
            method: 'POST',
            file: files,
            fileFormDataName: fileFieldNames,
            fields: {
              "executorcount": executorNum,
              "args": args
            }
          });

          upload.then(function (response) {
            if (onComplete) {
              var data = response.data;
              onComplete({success: data && data.success});
            }
          }, function (response) {
            if (onComplete) {
              onComplete(decodeErrorResponse(response.data));
            }
          });
        },

        /** Submit an user defined application with user configuration */
        submitDag: function (args, onComplete) {
          var url = restapiV1Root + 'master/submitdag';
          return $http.post(url, args).then(function (response) {
            if (onComplete) {
              onComplete(decodeSuccessResponse(response.data));
            }
          }, function (response) {
            if (onComplete) {
              onComplete(decodeErrorResponse(response.data));
            }
          });
        },

        /** Upload a set of JAR files */
        uploadJars: function (files, onComplete) {
          var upload = Upload.upload({
            url: restapiV1Root + 'master/uploadjar',
            method: 'POST',
            file: files,
            fileFormDataName: 'jar'
          });

          upload.then(function (response) {
            if (onComplete) {
              onComplete(decodeSuccessResponse({files: response.data}));
            }
          }, function (response) {
            if (onComplete) {
              onComplete(decodeErrorResponse(response.data));
            }
          });
        },

        /** Add a new worker */
        addWorker: function (onComplete) {
          var count = 1;
          var url = restapiV1Root + 'supervisor/addworker/' + count;
          return $http.post(url).then(function (response) {
            if (angular.isFunction(onComplete)) {
              onComplete(decodeSuccessResponse(response.data));
            }
          }, function (response) {
            if (angular.isFunction(onComplete)) {
              onComplete(decodeErrorResponse(response.data));
            }
          });
        },

        /** Remove a new worker */
        removeWorker: function (workerId, onComplete) {
          var url = restapiV1Root + 'supervisor/removeworker/' + workerId;
          return $http.post(url).then(function (response) {
            if (angular.isFunction(onComplete)) {
              onComplete(decodeSuccessResponse(response.data));
            }
          }, function (response) {
            if (angular.isFunction(onComplete)) {
              onComplete(decodeErrorResponse(response.data));
            }
          });
        },

        /** Replace a dag processor at runtime */
        replaceDagProcessor: function (files, formFormNames, appId, oldProcessorId, newProcessorDescription, inheritConf, onComplete) {
          var url = restapiV1Root + 'appmaster/' + appId + '/dynamicdag';
          var args = {
            "$type": 'io.gearpump.streaming.appmaster.DagManager.ReplaceProcessor',
            oldProcessorId: oldProcessorId,
            newProcessorDescription: angular.merge({
              id: oldProcessorId
            }, newProcessorDescription),
            inheritConf: inheritConf
          };
          url += '?args=' + encodeURIComponent(angular.toJson(args));

          var promise;
          var filtered = _.filter(files, function (file) {
            return file;
          });
          if (filtered.length) {
            promise = Upload.upload({
              url: url,
              method: 'POST',
              file: filtered,
              fileFormDataName: formFormNames
            });
          } else {
            promise = $http.post(url);
          }

          promise.then(function () {
            if (onComplete) {
              onComplete({success: true});
            }
          }, function (response) {
            if (onComplete) {
              onComplete({success: false, reason: response.data});
            }
          });
        },

        /** Return the service version in onData callback */
        serviceVersion: function (onData) {
          return $http.get(conf.restapiRoot + 'version').then(function (response) {
            if (angular.isFunction(onData)) {
              onData(response.data);
            }
          });
        }
      };
      return self;
    }
  ])
;
