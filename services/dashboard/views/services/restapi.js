/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.restapi', [])

  .factory('restapi', ['$http', '$timeout', '$modal', 'conf', function ($http, $timeout, $modal, conf) {

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
                noticeWindow.$promise.then(noticeWindow.hide);
                shouldCancel = !onData || onData(response.data);
              }
            }, function (response) {
              if (!shouldCancel) {
                noticeWindow.$promise.then(noticeWindow.show);
              }
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

      /** Return the config link of an application */
      appConfigLink: function(appId) {
        return conf.restapiRoot + '/config/app/' + appId;
      },

      /** Return the config link of a worker */
      workerConfigLink: function(workerId) {
        return conf.restapiRoot + '/config/worker/' + workerId;
      },

      /** Return the version of Gearpump */
      getVersion: function() {
        var url = conf.restapiRoot + '/version';
        return $http.get(url);
      },

      /** Return the config link of the master */
      masterConfigLink: function() {
        return conf.restapiRoot + '/config/master';
      }
    };
  }])
;
