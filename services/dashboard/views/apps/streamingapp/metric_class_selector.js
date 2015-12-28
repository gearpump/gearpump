/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .directive('metricClassSelector', function() {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'views/apps/streamingapp/metric_class_selector.html',
      scope: {
        metricClass: '=',
        metricType: '=',
        metricName: '='
      },
      link: function(scope) {
        'use strict';

        var metricClassLookup = {
          'Message Send Throughput': {type: 'meter', class: 'sendThroughput', name: 'Message Send Throughput'},
          'Message Receive Throughput': {type: 'meter', class: 'receiveThroughput', name: 'Message Receive Throughput'},
          'Average Message Processing Time': {type: 'histogram', class: 'processTime', name: 'Message Average Processing Time'},
          'Average Message Receive Latency': {type: 'histogram', class: 'receiveLatency', name: 'Message Average Receive Latency'}
        };

        scope.names = {available: _.keys(metricClassLookup)};
        scope.names.selected = scope.names.available[1]; // use Receive Throughput by default
        scope.types = function(name) {
          return metricClassLookup[name].type;
        };

        scope.$watch('names.selected', function(val) {
          scope.metricClass = metricClassLookup[val].class;
          scope.metricType = metricClassLookup[val].type;
          scope.metricName = metricClassLookup[val].name;
        });
      }
    }
  })
;