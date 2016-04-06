/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
angular.module('dashboard')

  .controller('AddWorkerCtrl', ['$scope', 'restapi', 'i18n',
    function ($scope, restapi, i18n) {
      'use strict';

      $scope.description = i18n.terminology.worker;
      $scope.count = 1;

      $scope.add = function () {
        $scope.adding = true;
        $scope.shouldNoticeSubmitFailed = false;
        return restapi.addWorker(
          function handleResponse(response) {
            $scope.shouldNoticeSubmitFailed = !response.success;
            $scope.adding = false;
            if (response.success) {
              $scope.$hide();
            } else {
              $scope.error = response.error;
            }
          },
          function handleException(ex) {
            $scope.shouldNoticeSubmitFailed = true;
            $scope.adding = false;
            $scope.error = ex;
          }
        );
      };
    }])
;