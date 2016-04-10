/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
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