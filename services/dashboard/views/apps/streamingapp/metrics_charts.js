/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .controller('StreamingAppMetricsChartsCtrl', ['$scope', '$element', 'conf',
    function($scope, elem, conf) {
      'use strict';

      var timeLabel = moment().format('HH:mm:ss');
      // todo: fill real historical data when possible (instead of zeros)
      var initialData = _.range(conf.metricsChartDataCount).map(function() {
        return {x: timeLabel, y: 0};
      });
      var lineChartOptionBase = {
        height: '108px',
        data: initialData,
        maxDataNum: conf.metricsChartDataCount,
        seriesNames: ['T1']
      };

      $scope.throughputChartOptions = angular.merge({
        valueFormatter: function(value) {
          return Number(value).toFixed(0) + ' msg/s';
        }
      }, lineChartOptionBase);
      $scope.durationChartOptions = angular.merge({
        valueFormatter: function(value) {
          return Number(value).toFixed(3) + ' ms';
        }
      }, lineChartOptionBase);

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
        $scope.averageTaskReceiveLatency = metricsProvider.getMessageReceiveLatency();
        $scope.averageProcessingTimeData = {x: timeLabel, y: $scope.averageProcessingTime};
        $scope.averageTaskReceiveLatencyData = {x: timeLabel, y: $scope.averageTaskReceiveLatency};
      }
    }])
;