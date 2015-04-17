/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.apps.appmaster')

  .controller('AppMetricsCtrl', ['$scope', function ($scope) {
    $scope.itemsByPage = 20;
    $scope.$watchCollection('streamingDag.meter.sendThroughput', function(array) {
      $scope.sendThroughputMetrics = d3.values(array);
    });
    $scope.$watchCollection('streamingDag.meter.receiveThroughput', function(array) {
      $scope.receiveThroughputMetrics = d3.values(array);
    });
    $scope.$watchCollection('streamingDag.histogram.processTime', function(array) {
      $scope.processTimeMetrics = d3.values(array);
    });
    $scope.$watchCollection('streamingDag.histogram.receiveLatency', function(array) {
      $scope.receiveLatencyMetrics = d3.values(array);
    });
  }])

  .filter('lpart', function () {
    return function (name) {
      var parts = name.split('.');
      return parts[parts.length - 1];
    };
  })
;
