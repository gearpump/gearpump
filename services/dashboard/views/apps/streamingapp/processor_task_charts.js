/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  // todo: refactoring required
  .controller('StreamingAppProcessorTaskChartsCtrl', ['$scope', 'conf', '$interval',
    function($scope, conf, $interval) {
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

        var lineChartOptionBase = {
          height: '108px',
          maxDataNum: conf.metricsChartDataCount,
          seriesNames: $scope.tasks.selected
        };

        var throughputChartOptions = angular.merge({
          valueFormatter: function(value) {
            return Number(value).toFixed(0) + ' msg/s';
          }
        }, lineChartOptionBase);

        var durationChartOptions = angular.merge({
          valueFormatter: function(value) {
            return Number(value).toFixed(3) + ' ms';
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

        function updateMetricsCharts(metricsProvider) {
          var processorId = $scope.processor.id;
          var timeLabel = moment().format('HH:mm:ss');
          var receivedMessages = metricsProvider.getReceivedMessages(processorId);
          var sentMessages = metricsProvider.getSentMessages(processorId);
          var messageProcessingTime = metricsProvider.getMessageProcessingTime(processorId);
          var messageReceiveLatency = metricsProvider.getMessageReceiveLatency(processorId);

          function filterUnselectedTasks(array) {
            return _.map($scope.selectedTaskIds, function(id) {
              return array[id];
            });
          }

          $scope.receiveMessageRateChart.data = [{
            x: timeLabel,
            y: filterUnselectedTasks(receivedMessages.rate)
          }];
          $scope.sendMessageRateChart.data = [{
            x: timeLabel,
            y: filterUnselectedTasks(sentMessages.rate)
          }];
          $scope.processingTimeChart.data = [{
            x: timeLabel,
            y: filterUnselectedTasks(messageProcessingTime)
          }];
          $scope.receiveLatencyChart.data = [{
            x: timeLabel,
            y: filterUnselectedTasks(messageReceiveLatency)
          }];
        }

        $scope.$watchCollection('dag', function(dag) {
          updateMetricsCharts($scope.dag);
        });
      });
    }])
;