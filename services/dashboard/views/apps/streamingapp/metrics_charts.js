/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .controller('StreamingAppMetricsChartsCtrl', ['$scope', 'conf',
    function($scope, conf) {
      'use strict';

      var lineChartOptionBase = {
        height: '108px',
        maxDataNum: conf.metricsChartDataCount
      };

      var throughputChartOptions = angular.merge({
        valueFormatter: function(value) {
          return Number(value).toFixed(0) + ' msg/s';
        },
        seriesNames: ['Sum of All Processors']
      }, lineChartOptionBase);

      $scope.sendThroughputChartOptions = angular.merge({
        data: _.map($scope.dag.getHistoricalMessageReceiveThroughput(), function(value, time) {
          return {x: moment(Number(time)).format('HH:mm:ss'), y: value};
        })
      }, throughputChartOptions);

      $scope.receiveThroughputChartOptions = angular.merge({
        data: _.map($scope.dag.getHistoricalMessageSendThroughput(), function(value, time) {
          return {x: moment(Number(time)).format('HH:mm:ss'), y: value};
        })
      }, throughputChartOptions);

      var durationChartOptions = angular.merge({
        valueFormatter: function(value) {
          return Number(value).toFixed(3) + ' ms';
        },
        seriesNames: ['Average of All Processors']
      }, lineChartOptionBase);

      $scope.averageProcessingTimeChartOptions = angular.merge({
        data: _.map($scope.dag.getHistoricalAverageMessageProcessingTime(), function(value, time) {
          return {x: moment(Number(time)).format('HH:mm:ss'), y: value};
        })
      }, durationChartOptions);

      $scope.averageMessageReceiveLatencyChartOptions = angular.merge({
        data: _.map($scope.dag.getHistoricalAverageMessageReceiveLatency(), function(value, time) {
          return {x: moment(Number(time)).format('HH:mm:ss'), y: value};
        })
      }, durationChartOptions);

      $scope.$watchCollection('dag.metrics.meter', function() {
        updateMeterMetricsCharts($scope.dag);
      });

      $scope.$watchCollection('dag.metrics.histogram', function() {
        updateHistogramMetricsCharts($scope.dag);
      });

      function updateMeterMetricsCharts(metricsProvider) {
        var timeLabel = moment().format('HH:mm:ss');
        var receivedMessages = metricsProvider.getReceivedMessages();
        var sentMessages = metricsProvider.getSentMessages();

        $scope.currentMessageSendRate = sentMessages.rate;
        $scope.currentMessageReceiveRate = receivedMessages.rate;
        $scope.totalSentMessages = sentMessages.total;
        $scope.totalReceivedMessages = receivedMessages.total;
        $scope.receiveThroughputData = {x: timeLabel, y: $scope.currentMessageReceiveRate};
        $scope.sendThroughputData = {x: timeLabel, y: $scope.currentMessageSendRate};
      }

      function updateHistogramMetricsCharts(metricsProvider) {
        var timeLabel = moment().format('HH:mm:ss');
        $scope.averageProcessingTime = metricsProvider.getMessageProcessingTime();
        $scope.averageMessageReceiveLatency = metricsProvider.getMessageReceiveLatency();
        $scope.averageProcessingTimeData = {x: timeLabel, y: $scope.averageProcessingTime};
        $scope.averageMessageReceiveLatencyData = {x: timeLabel, y: $scope.averageMessageReceiveLatency};
      }
    }])
;