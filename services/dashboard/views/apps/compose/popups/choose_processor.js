/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .controller('ComposeAppChooseProcessorCtrl', ['$scope',
    function ($scope) {
      'use strict';

      $scope.invalid = {};
      $scope.processor = angular.merge({
        parallelism: 1
      }, $scope.processor);

      $scope.canSave = function () {
        return _.sum($scope.invalid) === 0;
      };

      $scope.save = function () {
        $scope.$hide();
        if ($scope.onChange) {
          $scope.onChange($scope.processor);
        }
      }
    }])
;