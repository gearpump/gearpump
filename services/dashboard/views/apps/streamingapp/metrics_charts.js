/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .directive('metricsCharts', function() {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'views/apps/streamingapp/metrics_charts.html',
      scope: true, // inherit parent scope
      controller: ['$scope', '$interval', 'helper', 'models', function($scope, $interval, helper, models) {
        'use strict';

        var metricsProvider = $scope.dag;
        var processorId;
        if ($scope.processor) {
          processorId = $scope.processor.id;
          $scope.sendThroughputMetricsCaption = 'Message Send Throughput';
          $scope.sendThroughputMetricsDescription = '';
          $scope.receiveThroughputMetricsCaption = 'Message Receive Throughput';
          $scope.receiveThroughputMetricsDescription = '';
        }
        var sc = $scope.metricsConfig;
        var recentChartPoints = sc.retainRecentDataSeconds * 1000 / sc.retainRecentDataIntervalMs;
        var histChartPoints = sc.retainHistoryDataHours * 3600 * 1000 / sc.retainHistoryDataIntervalMs;
        recentChartPoints--; // ProcessorFilter will actually reduce one point
        histChartPoints--;
        var updateRecentMetricsPromise;
        $scope.$on('$destroy', function() {
          $interval.cancel(updateRecentMetricsPromise);
        });

        // part 1
        var lineChartOptionBase = {
          height: '108px',
          visibleDataPointsNum: recentChartPoints,
          data: _.times(recentChartPoints, function() {
            return {x: '', y: '-'};
          })
        };

        var throughputChartOptions = angular.merge({
          valueFormatter: function(value) {
            return helper.metricValue(value) + ' msg/s';
          },
          seriesNames: ['Throughput']
        }, lineChartOptionBase);

        $scope.sendThroughputChartOptions = angular.copy(throughputChartOptions);
        $scope.receiveThroughputChartOptions = angular.copy(throughputChartOptions);

        var durationChartOptions = angular.merge({
          valueFormatter: function(value) {
            return helper.metricValue(value) + ' ms';
          },
          seriesNames: ['Duration']
        }, lineChartOptionBase);

        $scope.averageProcessingTimeChartOptions = angular.copy(durationChartOptions);
        $scope.averageMessageReceiveLatencyChartOptions = angular.copy(durationChartOptions);

        function redrawMetricsCharts() {

          function _batch(chartNameBase, fnName, data) {
            // set null will rebuild historical chart
            $scope[chartNameBase + 'HistChartOptions'] = null;

            var chartData = metricsToChartData(metricsProvider[fnName](data,
              $scope.metricsConfig.retainRecentDataIntervalMs, processorId));
            if ($scope.showCurrentMetrics) {
              $scope[chartNameBase + 'Data'] = chartData;
            } else {
              $scope[chartNameBase + 'HistChartOptions'] =
                angular.extend({}, $scope[chartNameBase+ 'ChartOptions'], {
                  visibleDataPointsNum: Math.max(chartData.length, histChartPoints),
                  data: chartData
                });
            }
          }

          if (!$scope.showCurrentMetrics) {
            $interval.cancel(updateRecentMetricsPromise);
          }

          var queryMetricsPromise = $scope.showCurrentMetrics ?
            models.$get.appMetrics($scope.app.appId, $scope.metricsConfig.retainRecentDataIntervalMs) :
            models.$get.appHistMetrics($scope.app.appId);
          queryMetricsPromise.then(function(metrics) {
            var data = metrics.$data();
            _batch('sendThroughput', 'toHistoricalMessageSendThroughputData', data);
            _batch('receiveThroughput', 'toHistoricalMessageReceiveThroughputData', data);
            _batch('averageProcessingTime', 'toHistoricalMessageAverageProcessingTimeData', data);
            _batch('averageMessageReceiveLatency', 'toHistoricalAverageMessageReceiveLatencyData', data);

            if ($scope.showCurrentMetrics) {
              updateRecentMetricsPromise = $interval(fillChartsWithCurrentMetrics,
                sc.retainRecentDataIntervalMs);
            }
          });
        }

        function fillChartsWithCurrentMetrics() {

          function _data(metric, metricTime) {
            var metrics = {};
            metrics[metricTime] = [metric];
            return metricsToChartData(metrics);
          }

          var timeResolution = $scope.metricsConfig.retainRecentDataIntervalMs;
          var metricTime = Math.floor(metricsProvider.metricsTime / timeResolution) * timeResolution;
          $scope.sendThroughputData = _data($scope.currentMessageSendRate, metricTime);
          $scope.receiveThroughputData = _data($scope.currentMessageReceiveRate, metricTime);
          $scope.averageProcessingTimeData = _data($scope.averageProcessingTime, metricTime);
          $scope.averageMessageReceiveLatencyData = _data($scope.averageMessageReceiveLatency, metricTime);
        }

        function metricsToChartData(metrics) {
          return _.map(metrics, function(value, timeString) {
            return {
              x: moment(Number(timeString)).format('HH:mm:ss'),
              y: value
            };
          });
        }

        // part 2
        function updateCurrentMeterMetrics() {
          var receivedMessages = metricsProvider.getReceivedMessages(processorId);
          var sentMessages = metricsProvider.getSentMessages(processorId);

          $scope.currentMessageSendRate = sentMessages.rate;
          $scope.currentMessageReceiveRate = receivedMessages.rate;
          $scope.totalSentMessages = sentMessages.total;
          $scope.totalReceivedMessages = receivedMessages.total;
        }

        function updateCurrentHistogramMetrics() {
          $scope.averageProcessingTime = metricsProvider.getMessageProcessingTime(processorId);
          $scope.averageMessageReceiveLatency = metricsProvider.getMessageReceiveLatency(processorId);
        }

        $scope.showCurrentMetrics = true;
        $scope.$watch('showCurrentMetrics', function(newVal, oldVal) {
          if (angular.equals(newVal, oldVal)) {
            return; // ignore initial notification
          }
          redrawMetricsCharts();
        });

        // common watching
        var initial = true;
        $scope.$watch('dag.metricsTime', function() {
          // part 1
          updateCurrentMeterMetrics();
          updateCurrentHistogramMetrics();

          // part 2
          if (initial) {
            // note that, the latest metrics do not contain enough points for drawing charts, so
            // we request recent metrics from server.
            redrawMetricsCharts();
            initial = false;
          }
        });
      }]
    };
  })
;