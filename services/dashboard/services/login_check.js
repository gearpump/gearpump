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
