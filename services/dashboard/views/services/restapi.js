/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.restapi', ['cfp.loadingBar'])

  .factory('restapi', ['$http', '$timeout', '$modal', 'Upload', 'conf', 'cfpLoadingBar',
    function ($http, $timeout, $modal, Upload, conf, cfpLoadingBar) {

    var noticeWindow = $modal({
      template: "views/services/serverproblemnotice.html",
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
        var url = conf.restapiRoot + '/internal/intermediate/delete/appmaster/' + appId;
        return $http.post(url);
      },

      /** Restart a running application and return a promise */
      restartAppAsync: function(appId) {
        var url = conf.restapiRoot + '/appmaster/' + appId + '/restart';
        return $http.post(url);
      },

      /** Return the config link of an application */
      appConfigLink: function(appId) {
        return conf.restapiRoot + '/config/app/' + appId;
      },

      /** Return the config link of a worker */
      workerConfigLink: function(workerId) {
        return conf.restapiRoot + '/config/worker/' + workerId;
      },

      /** Return the config link of the master */
      masterConfigLink: function() {
        return conf.restapiRoot + '/config/master';
      },

      submitUserApp: function(file, onComplete) {
        cfpLoadingBar.start();

        var upload = Upload.upload({
          url: conf.restapiRoot + '/userapp/submit',
          method: 'POST',
          headers: {},
          fields: {},
          file: file
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
        }).finally(function() {
          cfpLoadingBar.complete();
        });

        upload.progress(function (evt) {
          // Math.min is to fix IE which reports 200% sometimes
          var progress = Math.min(1, parseInt(evt.loaded / evt.total));
          cfpLoadingBar.set(progress);
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
          $http.get(conf.restapiRoot + '/version')
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
