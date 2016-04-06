/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')
// Authentication Angular interceptor for http methods.
// If server respond 401 (unauthenticated), will redirect to login page.
  .factory('authInterceptor', ['$q', 'conf', function ($q, conf) {

    // Defer the error response to caller after this timeout to avoid browser hang issue
    // See https://github.com/gearpump/gearpump/issues/1855
    var deferErrorResponseMs = 3000;
    return {
      'responseError': function (response) {
        if (response.status == 401) {
          window.location.href = conf.loginUrl;
        }

        var deferred = $q.defer();
        setTimeout(function () {
          deferred.reject(response);
        }, 3000);
        return deferred.promise;

      }
    };
  }])
  .config(['$httpProvider', function ($httpProvider) {
    $httpProvider.interceptors.push('authInterceptor');
  }]);