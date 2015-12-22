/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  // todo: refactoring required
  .controller('StreamingAppProcessorTaskChartsCtrl', ['$scope', '$filter',
    function($scope, $filter) {
      'use strict';

      // rebuild the charts when `tasks` is changed.
      $scope.$watchCollection('tasks', function(tasks) {
        if (!tasks.selected) {
          console.warn('should not load this page');
          return;
        }
        $scope.selectedTaskIds = _.map($scope.tasks.selected, function(taskName) {
          return parseInt(taskName.substr(1));
        });

        var sc = $scope.metricsConfig;
        var currentChartPoints = sc.retainRecentDataSeconds * 1000 / sc.retainRecentDataIntervalMs;
        var lineChartOptionBase = {
          height: '108px',
          visibleDataPointsNum: currentChartPoints,
          data: _.times(currentChartPoints, function() {
            return {x: '', y: '-'};
          }),
          seriesNames: $scope.tasks.selected
        };

        var throughputChartOptions = angular.merge({
          valueFormatter: function(value) {
            return $filter('number')(value, 0) + ' msg/s';
          }
        }, lineChartOptionBase);

        var durationChartOptions = angular.merge({
          valueFormatter: function(value) {
            return $filter('number')(value, 3) + ' ms';
          }
        }, lineChartOptionBase);

        $scope.receiveMessageRateChart = {
          options: throughputChartOptions
        };
        $scope.sendMessageRateChart = {
          options: throughputChartOptions
        };
        $scope.processingTimeChart = {
          options: durationChartOptions
        };
        $scope.receiveLatencyChart = {
          options: durationChartOptions
        };

        $scope.$watchCollection('metrics', function(metrics) {
          if (angular.isObject(metrics)) {
            updateMetricsCharts(metrics);
          }
        });

        function updateMetricsCharts(metrics) {
          var timeLabel = moment().format('HH:mm:ss');
          $scope.receiveMessageRateChart.data = [{
            x: timeLabel,
            y: extractMessageThroughput(metrics.receiveThroughput)
          }];
          $scope.sendMessageRateChart.data = [{
            x: timeLabel,
            y: extractMessageThroughput(metrics.sendThroughput)
          }];
          $scope.processingTimeChart.data = [{
            x: timeLabel,
            y: extractTimeAverage(metrics.processTime)
          }];
          $scope.receiveLatencyChart.data = [{
            x: timeLabel,
            y: extractTimeAverage(metrics.receiveLatency)
          }];
        }

        function extractMessageThroughput(metrics) {
          return extractSelectedMetricField(metrics, 'meanRate');
        }

        function extractTimeAverage(metrics) {
          return extractSelectedMetricField(metrics, 'mean');
        }

        function extractSelectedMetricField(metrics, field) {
          return _.map($scope.selectedTaskIds, function(taskId) {
            return metrics[taskId].values[field];
          });
        }
      });
    }])
;