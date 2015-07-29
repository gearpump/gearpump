/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.apps.appmaster')

  .controller('AppMetricsCtrl', ['$scope', function ($scope) {
    $scope.itemsByPage = 15;

    var lookup = {
      'streamingDag.meter.receiveThroughput': 'Receive Throughput',
      'streamingDag.meter.sendThroughput': 'Send Throughput',
      'streamingDag.histogram.processTime': 'Processing Time',
      'streamingDag.histogram.receiveLatency': 'Receive Latency'
    };
    $scope.names = {available: d3.values(lookup)};
    $scope.names.selected = $scope.names.available[0];

    function getMetricsClassByLabel(label) {
      var i = $scope.names.available.indexOf(label);
      if (i !== -1) {
        return d3.keys(lookup)[i];
      }
      return '';
    }

    $scope.isMeter = function() {
      return getMetricsClassByLabel($scope.names.selected).indexOf('.meter.') > 0;
    }

    var watchFn = null;
    $scope.$watch('names.selected', function (newVal) {
      if (watchFn) {
        watchFn();
        watchFn = null;
      }
      var clazz = getMetricsClassByLabel(newVal);
      if (clazz) {
        watchFn =
          $scope.$watchCollection(clazz, function (array) {
            $scope.metrics = d3.values(array);
          });
      } else {
        $scope.metrics = [];
      }
    });
  }])

  .filter('lpart', function () {
    return function (name) {
      var parts = name.split('.');
      return parts[parts.length - 1];
    };
  })
;
