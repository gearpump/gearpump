/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .controller('StreamingAppMetricsChartsCtrl', ['$scope', '$filter', 'sorted', 'conf',
    function($scope, $filter, sorted, conf) {
      'use strict';

      var lineChartOptionBase = {
        height: '108px',
        visibleDataPointsNum: conf.metricsDataPointsP5M,
        data: _.times(conf.metricsDataPointsP5M, function() {
          return {x: '', y: '-'};
        })
      };

      var throughputChartOptions = angular.merge({
        valueFormatter: function(value) {
          return $filter('number')(value, 0) + ' msg/s';
        },
        seriesNames: ['Throughput']
      }, lineChartOptionBase);

      $scope.sendThroughputChartOptions = angular.copy(throughputChartOptions);
      $scope.receiveThroughputChartOptions = angular.copy(throughputChartOptions);

      var durationChartOptions = angular.merge({
        valueFormatter: function(value) {
          return $filter('number')(value, 3) + ' ms';
        },
        seriesNames: ['Duration']
      }, lineChartOptionBase);

      $scope.averageProcessingTimeChartOptions = angular.copy(durationChartOptions);
      $scope.averageMessageReceiveLatencyChartOptions = angular.copy(durationChartOptions);

      $scope.$watchCollection('dag.metricsTime', function() {
        updateCurrentMeterMetrics($scope.dag);
        updateCurrentHistogramMetrics($scope.dag);
      });

      $scope.$watchCollection('metrics', function(metrics) {
        if (angular.isObject(metrics)) {
          updateHistoricalMetricsCharts(metrics);
        }
      });

      function updateHistoricalMetricsCharts(metrics) {
        $scope.sendThroughputData = buildChartData('toHistoricalMessageSendThroughputData', metrics);
        $scope.receiveThroughputData = buildChartData('toHistoricalMessageReceiveThroughputData', metrics);
        $scope.averageProcessingTimeData = buildChartData('toHistoricalMessageAverageProcessingTimeData', metrics);
        $scope.averageMessageReceiveLatencyData = buildChartData('toHistoricalAverageMessageReceiveLatencyData', metrics);
      }

      function buildChartData(convertFuncName, metrics) {
        return _.map(sorted.byKey($scope.dag[convertFuncName](metrics)),
          function(value, timeString) {
            return {
              x: moment(Number(timeString)).format('HH:mm:ss'),
              y: value
            };
          });
      }

      function updateCurrentMeterMetrics(metricsProvider) {
        var receivedMessages = metricsProvider.getReceivedMessages();
        var sentMessages = metricsProvider.getSentMessages();

        $scope.currentMessageSendRate = sentMessages.rate;
        $scope.currentMessageReceiveRate = receivedMessages.rate;
        $scope.totalSentMessages = sentMessages.total;
        $scope.totalReceivedMessages = receivedMessages.total;
      }

      function updateCurrentHistogramMetrics(metricsProvider) {
        $scope.averageProcessingTime = metricsProvider.getMessageProcessingTime();
        $scope.averageMessageReceiveLatency = metricsProvider.getMessageReceiveLatency();
      }
    }])
;