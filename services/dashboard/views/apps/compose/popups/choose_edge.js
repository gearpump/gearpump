/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .controller('ComposeAppChooseEdgeCtrl', ['$scope',
    function ($scope) {
      'use strict';

      $scope.invalid = {};
      $scope.edge = angular.merge({}, $scope.edge);
      $scope.partitioners = $scope.partitioners.map(function (partitioner) {
        return {
          text: partitioner,
          icon: 'glyphicon glyphicon-random'
        };
      });

      $scope.canSave = function () {
        return _.sum($scope.invalid) === 0 &&
            // todo: check ring graph condition
          ($scope.edge.from.value !== $scope.edge.to.value);
      };

      $scope.save = function () {
        $scope.$hide();
        if ($scope.onChange) {
          $scope.edge.from = $scope.edge.from.value;
          $scope.edge.to = $scope.edge.to.value;
          $scope.edge.id = $scope.edge.from + '_' + $scope.edge.to;
          $scope.onChange($scope.edge);
        }
      }
    }])
;