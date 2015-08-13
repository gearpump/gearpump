/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  // todo: refactoring required
  .controller('StreamingAppProcessorTaskChartsCtrl', ['$scope', 'conf',
    function($scope, conf) {
      'use strict';

      if (!$scope.tasks.selected) {
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

        $scope.currentMessageSendRate = sentMessages.rate;
        $scope.currentMessageReceiveRate = receivedMessages.rate;
        $scope.totalSentMessages = sentMessages.total;
        $scope.totalReceivedMessages = receivedMessages.total;
        $scope.averageProcessingTime = metricsProvider.getMessageProcessingTime(processorId);
        $scope.averageTaskReceiveLatency = metricsProvider.getMessageReceiveLatency(processorId);

        function filterUnselectedTasks(array) {
          return _.map($scope.selectedTaskIds, function(id) {
            return array[id];
          });
        }

        $scope.receiveMessageRateChart.data = [{
          x: timeLabel,
          y: filterUnselectedTasks($scope.currentMessageReceiveRate)
        }];
        $scope.sendMessageRateChart.data = [{
          x: timeLabel,
          y: filterUnselectedTasks($scope.currentMessageSendRate)
        }];
        $scope.processingTimeChart.data = [{
          x: timeLabel,
          y: filterUnselectedTasks($scope.averageProcessingTime)
        }];
        $scope.receiveLatencyChart.data = [{
          x: timeLabel,
          y: filterUnselectedTasks($scope.averageTaskReceiveLatency)
        }];
      }

      $scope.$watchCollection('dag', function(dag) {
        updateMetricsCharts(dag);
      });
    }])
;