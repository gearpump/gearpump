/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.restapi', [])

  .factory('restapi', ['$http', '$timeout', 'conf', function ($http, $timeout, conf) {
    return {
      /** Get data from server periodically before a scope is destroyed. */
      subscribe: function (url, scope, onSuccess, onError) {
        // TODO: convert to websocket push model
        var timeoutPromise;
        scope.$on('$destroy', function () {
          $timeout.cancel(timeoutPromise);
        });
        var fn = function () {
          var cancel = false;
          $http.get(conf.restapiRoot + url)
            .success(function (data) {
              cancel = !onSuccess || onSuccess(data);
            })
            .error(function (reason, code) {
              // TODO: show error dialog or notification when server is disconnected (#602)
              cancel = !onError || onError(reason, code);
            })
            .finally(function () {
              if (!cancel) {
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
          },
          function (reason, code) {
            return !onData || onData(null);
          });
      },

      /** Kill a running application */
      killApp: function(appId) {
        var url = conf.restapiRoot + '/appmaster/' + appId;
        return $http.delete(url);
      }
    };
  }])
;
