/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

/** TODO: refactoring work required */
  .factory('restapi', ['$http', '$timeout', '$modal', 'Upload', 'conf',
    function($http, $timeout, $modal, Upload, conf) {
      'use strict';

      var noticeWindow = $modal({
        templateUrl: 'services/service_unreachable_notice.html',
        backdrop: 'static',
        show: false
      });

      var restapiV1Root = conf.restapiRoot + '/api/' + conf.restapiProtocol;
      return {
        /**
         * Retrieve data from rest service endpoint (HTTP GET) periodically in an angular scope.
         */
        subscribe: function(path, scope, onData, interval) {
          var timeoutPromise;
          var shouldCancel = false;
          scope.$on('$destroy', function() {
            shouldCancel = true;
            $timeout.cancel(timeoutPromise);
          });

          interval = interval || conf.restapiQueryInterval;
          var fn = function() {
            $http.get(restapiV1Root + path)
              .then(function(response) {
                if (!shouldCancel) {
                  shouldCancel = !onData || onData(response.data);
                }
              }, function(response) {
              })
              .finally(function() {
                if (!shouldCancel) {
                  timeoutPromise = $timeout(fn, interval);
                }
              });
          };
          timeoutPromise = $timeout(fn, interval);
        },

        get: function(path) {
          return $http.get(restapiV1Root + path);
        },

        /** Get data from server periodically until an user cancellation or scope exit. */
        repeatUntil: function(url, scope, onData) {
          // TODO: Once `subscribe` is turned to websocket push model, there is no need to have this method
          this.subscribe(url, scope,
            function(data) {
              return !onData || onData(data);
            });
        },

        /** Kill a running application */
        killApp: function(appId) {
          var url = restapiV1Root + '/appmaster/' + appId;
          return $http.delete(url);
        },

        /** Restart a running application and return a promise */
        restartAppAsync: function(appId) {
          var url = restapiV1Root + '/appmaster/' + appId + '/restart';
          return $http.post(url);
        },

        /** Return the config link of an application */
        appConfigLink: function(appId) {
          return restapiV1Root + '/appmaster/' + appId + '/config';
        },

        /** Return the config link of an application */
        appExecutorConfigLink: function(appId, executorId) {
          return restapiV1Root + '/appmaster/' + appId + '/executor/' + executorId + '/config';
        },

        /** Return the config link of a worker */
        workerConfigLink: function(workerId) {
          return restapiV1Root + '/worker/' + workerId + '/config';
        },

        /** Return the config link of the master */
        masterConfigLink: function() {
          return restapiV1Root + '/master/config';
        },

        /** Submit an user defined application with user configuration */
        submitUserApp: function(files, formFormNames, args, onComplete) {
          var params = args ? '?args=' + encodeURIComponent(args) : '';
          var upload = Upload.upload({
            url: restapiV1Root + '/master/submitapp' + params,
            method: 'POST',
            file: files,
            fileFormDataName: formFormNames
          });

          upload.then(function(response) {
            if (onComplete) {
              var data = response.data;
              onComplete({success: data && data.success});
            }
          }, function() {
            if (onComplete) {
              onComplete({success: false});
            }
          });
        },

        /** Replace a dag processor at runtime */
        replaceDagProcessor: function(files, formFormNames, appId, oldProcessorId, newProcessorDescription, onComplete) {
          var url = restapiV1Root + '/appmaster/' + appId + '/dynamicdag';
          var args = {
            "$type": 'io.gearpump.streaming.appmaster.DagManager.ReplaceProcessor',
            oldProcessorId: oldProcessorId,
            newProcessorDescription: angular.merge({
              id: oldProcessorId
            }, newProcessorDescription)
          };
          url += '?args=' + encodeURIComponent(angular.toJson(args));

          var promise;
          var filtered = _.filter(files, function(file) {
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

          promise.then(function() {
            if (onComplete) {
              onComplete({success: true});
            }
          }, function(response) {
            if (onComplete) {
              onComplete({success: false, reason: response.data});
            }
          });
        },

        /** Periodically check health. In case of problems show a notice window */
        repeatHealthCheck: function(scope, onData) {
          var timeoutPromise;
          var shouldCancel = false;
          scope.$on('$destroy', function() {
            shouldCancel = true;
            $timeout.cancel(timeoutPromise);
          });
          var fn = function() {
            $http.get(conf.restapiRoot + '/version')
              .then(function(response) {
                var serviceWasUnreachable = noticeWindow.$isShown;
                noticeWindow.$promise.then(noticeWindow.hide);
                if (serviceWasUnreachable) {
                  // todo: should find an elegant way to handling resolve failures and service issues
                  location.reload();
                  return;
                }
                if (onData) {
                  onData(response.data);
                }
              }, function() {
                noticeWindow.$promise.then(noticeWindow.show);
              })
              .finally(function() {
                if (!shouldCancel) {
                  timeoutPromise = $timeout(fn, conf.restapiQueryInterval);
                }
              });
          };
          fn();
        }
      };
    }
  ])
;
