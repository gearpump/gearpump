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

/**
 * The service will continuously contact service endpoint. A dialog will be shown, when the service is unreachable.
 */
  .service('HealthCheckService', ['$http', function ($http) {
    'use strict';

    var service = this;
    service._available = true;

    service.config = function (checkUrl, checkInterval, showDialogFn, hideDialogFn) {
      this.checkUrl = checkUrl;
      this.checkInterval = checkInterval;
      this.showDialogFn = showDialogFn;
      this.hideDialogFn = hideDialogFn;
    };

    service.isServiceAvailable = function () {
      return service._available;
    };

    service.checkForever = function () {
      var fn = function () {
        service._check().finally(
          function retry() {
            _.delay(fn, service.checkInterval);
          });
      };
      fn();
    };

    service._check = function () {
      return $http.get(service.checkUrl).then(
        function handleSuccess() {
          if (!service.isServiceAvailable()) {
            service._setServiceAvailable(true);
          }
        },
        function handleFailure() {
          service._setServiceAvailable(false);
        }
      );
    };

    service._setServiceAvailable = function (available) {
      service._available = available;
      if (available) {
        service.hideDialogFn();
      } else {
        service.showDialogFn();
      }
    };
  }])
;
