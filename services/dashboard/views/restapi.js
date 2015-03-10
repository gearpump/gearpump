/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.restapi', [])

  .factory('restapi', ['$http', '$timeout', 'conf', function ($http, $timeout, conf) {
    return {
      /** Fetch data from server periodically before a scope is destroyed. */
      subscribe: function (url, scope, onSuccess, onError) {
        var timeoutPromise;
        scope.$on('$destroy', function () {
          $timeout.cancel(timeoutPromise);
        });
        var fn = function () {
          var cancel = false;
          $http.get(conf.restapiRoot + url)
            .success(function (data) {
              cancel = onSuccess(data);
            })
            .error(function (message, code) {
              // TODO: auto reconnect to serve and block ui
              cancel = onError(message, code);
            })
            .finally(function () {
              if (!cancel) {
                timeoutPromise = $timeout(fn, conf.restapiAutoRefreshInterval);
              }
            });
        };
        fn();
      },

      kill: function(appId) {
        var url = conf.restapiRoot + '/appmaster/' + appId;
        return $http.delete(url);
      },

      metrics: function(appId) {
        var url = conf.restapiRoot + '/metrics/app/' + appId + '/app' + appId;
        return $http.get(url);
      }
    };
  }])
;
