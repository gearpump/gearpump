/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.restapi', [])

  .factory('restapi', ['$http', '$timeout', '$modal', 'Upload', 'conf',
    function ($http, $timeout, $modal, Upload, conf) {

    var noticeWindow = $modal({
      templateUrl: "views/services/serverproblemnotice.html",
      backdrop: 'static',
      show: false
    });

    return {
      /** Get data from server periodically before a scope is destroyed. */
      subscribe: function (url, scope, onData) {
        // TODO: convert to websocket push model
        var timeoutPromise;
        var shouldCancel = false;
        scope.$on('$destroy', function () {
          shouldCancel = true;
          $timeout.cancel(timeoutPromise);
        });

        var fn = function () {
          $http.get(conf.restapiRoot + url)
            .then(function (response) {
              if (!shouldCancel) {
                shouldCancel = !onData || onData(response.data);
              }
            }, function (response) {
            })
            .finally(function () {
              if (!shouldCancel) {
                timeoutPromise = $timeout(fn, conf.restapiAutoRefreshInterval);
              }
            });
        };
        fn();
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
      killApp: function(appId) {
        var url = conf.restapiRoot + '/appmaster/' + appId;
        return $http.delete(url);
      },

      /** Restart a running application and return a promise */
      restartAppAsync: function(appId) {
        var url = conf.restapiRoot + '/appmaster/' + appId + '/restart';
        return $http.post(url);
      },

      /** Return the config link of an application */
      appConfigLink: function(appId) {
        return conf.restapiRoot + '/appmaster/' + appId + '/config';
      },

      /** Return the config link of a worker */
      workerConfigLink: function(workerId) {
        return conf.restapiRoot + '/worker/' + workerId +  '/config';
      },

      /** Return the config link of the master */
      masterConfigLink: function() {
        return conf.restapiRoot + '/master/config';
      },

      /** Submit an user defined application with user configuration */
      submitUserApp: function(files, formFormNames, args, onComplete) {
        var params = args ? '?args=' + encodeURIComponent(args) : '';
        var upload = Upload.upload({
          url: conf.restapiRoot + '/master/submitapp' + params,
          method: 'POST',
          file: files,
          fileFormDataName: formFormNames
        });

        upload.then(function (response) {
          if (onComplete) {
            var data = response.data;
            onComplete({success: data && data.success});
          }
        }, function () {
          if (onComplete) {
            onComplete({success: false});
          }
        });
      },

      /** Replace a dag processor at runtime */
      replaceDagProcessor: function(appId, oldProcessorId, newProcessorDescription, onComplete) {
        var url = conf.restapiRoot + '/appmaster/' + appId + '/dynamicdag';
        var args = {
            "$type": 'org.apache.gearpump.streaming.appmaster.DagManager.ReplaceProcessor',
            oldProcessorId: oldProcessorId,
            newProcessorDescription: angular.merge({
              id: oldProcessorId
            }, newProcessorDescription)
          };
        $http.post(url, args).then(function (response) {
          if (onComplete) {
            onComplete({success: true});
          }
        }, function (response) {
          if (onComplete) {
            onComplete({success: false, reason: response.data});
          }
        });
      },

      /** Periodically check health. In case of problems show a notice window */
      repeatHealthCheck: function(scope, onData) {
        var timeoutPromise;
        var shouldCancel = false;
        scope.$on('$destroy', function () {
          shouldCancel = true;
          $timeout.cancel(timeoutPromise);
        });
        var fn = function () {
          $http.get(conf.root + 'version')
            .then(function (response) {
              noticeWindow.$promise.then(noticeWindow.hide);
              if (onData) {
                onData(response.data);
              }
            }, function () {
              noticeWindow.$promise.then(noticeWindow.show);
            })
            .finally(function () {
              if (!shouldCancel) {
                timeoutPromise = $timeout(fn, conf.restapiAutoRefreshInterval);
              }
            });
        };
        fn();
      }
    };
  }])
;
