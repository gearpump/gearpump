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
        $scope.chartGridClass = 'col-sm-12';
        if ($scope.processor) {
          processorId = $scope.processor.id;
          $scope.chartGridClass = 'col-sm-6';
          $scope.sendThroughputMetricsCaption = 'Message Send Throughput';
          $scope.sendThroughputMetricsDescription = '';
          $scope.receiveThroughputMetricsCaption = 'Message Receive Throughput';
          $scope.receiveThroughputMetricsDescription = '';
          $scope.messageLatencyMetricsCaption = 'Average Message Receive Latency';
          $scope.messageLatencyMetricsDescription = '';
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
            return helper.readableMetricValue(value) + ' msg/s';
          },
          seriesNames: ['']
        }, lineChartOptionBase);

        $scope.sendThroughputChartOptions = angular.copy(throughputChartOptions);
        $scope.receiveThroughputChartOptions = angular.copy(throughputChartOptions);

        var durationChartOptions = angular.merge({
          valueFormatter: function(value) {
            return helper.readableMetricValue(value) + ' ms';
          },
          seriesNames: ['']
        }, lineChartOptionBase);

        $scope.averageProcessingTimeChartOptions = angular.copy(durationChartOptions);
        $scope.messageReceiveLatencyChartOptions = angular.copy(durationChartOptions);

        function redrawMetricsCharts() {

          function _batch(chartNameBase, fnName, arg) {
            // set null will rebuild historical chart
            $scope[chartNameBase + 'HistChartOptions'] = null;

            var chartData = metricsToChartData(metricsProvider[fnName](arg));
            if ($scope.showCurrentMetrics) {
              $scope[chartNameBase + 'Data'] = chartData;
            } else {
              $scope[chartNameBase + 'HistChartOptions'] =
                angular.extend({}, $scope[chartNameBase+ 'ChartOptions'], {
                  visibleDataPointsNum: chartData.length ?
                    Math.max(chartData.length, histChartPoints) : 0, // 0 means to show "no data" animation
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
            var timeResolution = $scope.showCurrentMetrics ?
              $scope.metricsConfig.retainRecentDataIntervalMs:
              $scope.metricsConfig.retainHistoryDataIntervalMs;
            metricsProvider.replaceHistoricalMetrics(data, timeResolution);

            if (angular.isNumber(processorId)) {
              _batch('sendThroughput', 'getProcessorHistoricalMessageSendThroughput', processorId);
              _batch('receiveThroughput', 'getProcessorHistoricalMessageReceiveThroughput', processorId);
              _batch('averageProcessingTime', 'getProcessorHistoricalAverageMessageProcessingTime', processorId);
              _batch('messageReceiveLatency', 'getProcessorHistoricalAverageMessageReceiveLatency', processorId);
            } else {
              _batch('sendThroughput', 'getSourceProcessorHistoricalMessageSendThroughput');
              _batch('receiveThroughput', 'getSinkProcessorHistoricalMessageReceiveThroughput');
              _batch('messageReceiveLatency', 'getHistoricalCriticalPathLatency');
            }

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
          var metricTime = Math.floor(metricsProvider.metricsUpdateTime / timeResolution) * timeResolution;
          $scope.sendThroughputData = _data($scope.currentMessageSendRate, metricTime);
          $scope.receiveThroughputData = _data($scope.currentMessageReceiveRate, metricTime);
          $scope.averageProcessingTimeData = _data($scope.averageProcessingTime, metricTime);
          $scope.messageReceiveLatencyData = _data($scope.messageReceiveLatency, metricTime);
        }

        function metricsToChartData(metrics) {
          return _.map(metrics, function(value, timeString) {
            return {
              x: helper.timeToChartTimeLabel(Number(timeString), /*shortForm=*/$scope.showCurrentMetrics),
              y: helper.metricRounded(value)
            };
          });
        }

        // part 2
        function updateMetricCards() {
          var sentMessages, receivedMessages;
          if (angular.isNumber(processorId)) {
            sentMessages = metricsProvider.getProcessorSentMessages(processorId);
            receivedMessages = metricsProvider.getProcessorReceivedMessages(processorId);
            $scope.averageProcessingTime = metricsProvider.getProcessorAverageMessageProcessingTime(processorId);
            $scope.messageReceiveLatency = metricsProvider.getProcessorMessageReceiveLatency(processorId);
          } else {
            sentMessages = metricsProvider.getSourceProcessorSentMessageTotalAndRate();
            receivedMessages = metricsProvider.getSinkProcessorReceivedMessageTotalAndRate();
            $scope.messageReceiveLatency = metricsProvider.getCriticalPathLatency();
          }
          $scope.currentMessageSendRate = sentMessages.rate;
          $scope.currentMessageReceiveRate = receivedMessages.rate;
          $scope.totalSentMessages = sentMessages.total;
          $scope.totalReceivedMessages = receivedMessages.total;
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
        $scope.$watch('dag.metricsUpdateTime', function() {
          // part 1
          updateMetricCards();

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