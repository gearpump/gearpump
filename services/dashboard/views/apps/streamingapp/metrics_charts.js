/*
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
angular.module('dashboard')

  .directive('metricsCharts', function () {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'views/apps/streamingapp/metrics_charts.html',
      scope: true, // inherit parent scope
      controller: ['$scope', '$interval', 'helper', 'models', function ($scope, $interval, helper, models) {
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
        $scope.$on('$destroy', function () {
          $interval.cancel(updateRecentMetricsPromise);
        });

        // part 1
        function createChart(unit, visibleDataPointsNum, dataPoints) {
          return {
            options: {
              height: '108px',
              margin: {right: 30},
              seriesNames: [''],
              yAxisLabelFormatter: helper.yAxisLabelFormatterWithoutValue0(),
              visibleDataPointsNum: visibleDataPointsNum,
              data: dataPoints,
              valueFormatter: function (value) {
                return helper.readableMetricValue(value) + ' ' + unit;
              }
            },
            data: null
          };
        }

        function createThroughputChart(visibleDataPointsNum, dataPoints) {
          return createChart('msg/s', visibleDataPointsNum, dataPoints);
        }

        function createDurationChart(visibleDataPointsNum, dataPoints) {
          return createChart('ms', visibleDataPointsNum, dataPoints);
        }

        $scope.sendThroughputChart = createThroughputChart(recentChartPoints);
        $scope.receiveThroughputChart = createThroughputChart(recentChartPoints);
        $scope.averageProcessingTimeChart = createDurationChart(recentChartPoints);
        $scope.messageReceiveLatencyChart = createDurationChart(recentChartPoints);

        function redrawMetricsCharts() {

          function _rebuildChartWithNewDataPoints(chartOptions, metrics) {
            var all = !$scope.isShowingCurrentMetrics;
            var dataPoints = metricsToChartData(metrics);
            var visibleDataPointsNum = all ?
              Math.max(dataPoints.length, histChartPoints) : recentChartPoints;
            if (all && dataPoints.length < 2) {
              // Hide chart data, if there is only one data point there, which looks very ugly.
              visibleDataPointsNum = 0;
              dataPoints = [];
            }
            // Rebuild the chart
            chartOptions.data = dataPoints;
            chartOptions.visibleDataPointsNum = visibleDataPointsNum;
          }

          if (!$scope.isShowingCurrentMetrics) {
            $interval.cancel(updateRecentMetricsPromise);
          }

          var queryMetricsPromise = $scope.isShowingCurrentMetrics ?
            models.$get.appMetrics($scope.app.appId, $scope.metricsConfig.retainRecentDataIntervalMs) :
            models.$get.appHistMetrics($scope.app.appId);

          queryMetricsPromise.then(function (metrics) {
            var data = metrics.$data();
            var timeResolution = $scope.isShowingCurrentMetrics ?
              $scope.metricsConfig.retainRecentDataIntervalMs :
              $scope.metricsConfig.retainHistoryDataIntervalMs;
            metricsProvider.replaceHistoricalMetrics(data, timeResolution);

            if (angular.isNumber(processorId)) {
              _rebuildChartWithNewDataPoints($scope.sendThroughputChart.options,
                metricsProvider.getProcessorHistoricalMessageSendThroughput(processorId));
              _rebuildChartWithNewDataPoints($scope.receiveThroughputChart.options,
                metricsProvider.getProcessorHistoricalMessageReceiveThroughput(processorId));
              _rebuildChartWithNewDataPoints($scope.averageProcessingTimeChart.options,
                metricsProvider.getProcessorHistoricalAverageMessageProcessingTime(processorId));
              _rebuildChartWithNewDataPoints($scope.messageReceiveLatencyChart.options,
                metricsProvider.getProcessorHistoricalAverageMessageReceiveLatency(processorId));
            } else {
              _rebuildChartWithNewDataPoints($scope.sendThroughputChart.options,
                metricsProvider.getSourceProcessorHistoricalMessageSendThroughput());
              _rebuildChartWithNewDataPoints($scope.receiveThroughputChart.options,
                metricsProvider.getSinkProcessorHistoricalMessageReceiveThroughput());
              _rebuildChartWithNewDataPoints($scope.messageReceiveLatencyChart.options,
                metricsProvider.getHistoricalCriticalPathLatency());
            }

            if ($scope.isShowingCurrentMetrics) {
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
          $scope.sendThroughputChart.data = _data($scope.currentMessageSendRate, metricTime);
          $scope.receiveThroughputChart.data = _data($scope.currentMessageReceiveRate, metricTime);
          $scope.averageProcessingTimeChart.data = _data($scope.averageProcessingTime, metricTime);
          $scope.messageReceiveLatencyChart.data = _data($scope.messageReceiveLatency, metricTime);
        }

        function metricsToChartData(metrics) {
          return _.map(metrics, function (value, timeString) {
            return {
              x: helper.timeToChartTimeLabel(Number(timeString), /*shortForm=*/$scope.isShowingCurrentMetrics),
              y: helper.metricRounded(value)
            };
          });
        }

        $scope.isShowingCurrentMetrics = true;
        $scope.$watch('isShowingCurrentMetrics', function (newVal, oldVal) {
          if (angular.equals(newVal, oldVal)) {
            return; // ignore initial notification
          }
          redrawMetricsCharts();
        });

        // common watching
        var initial = true;
        $scope.$watch('dag.metricsUpdateTime', function () {
          if (initial) {
            // note that, the latest metrics do not contain enough points for drawing charts, so
            // we request recent metrics from server.
            redrawMetricsCharts();
            initial = false;
          }

          updateMetricCards();
        });

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
      }]
    };
  })
;
