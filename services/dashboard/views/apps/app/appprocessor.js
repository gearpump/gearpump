/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.apps.appmaster')

  .controller('AppProcessorCtrl', ['$scope', '$interval', 'conf', function ($scope, $interval, conf) {
    if ($scope.activeProcessorId === undefined) {
      return;
    }
    var activeProcessorId = Number($scope.activeProcessorId);
    var processor = $scope.streamingDag.processors[activeProcessorId];
    $scope.taskClass = processor.taskClass;
    $scope.description = processor.description;
    $scope.parallelism = processor.parallelism;
    var connections = $scope.streamingDag.calculateProcessorConnections(activeProcessorId);
    $scope.inputs = connections.inputs;
    $scope.outputs = connections.outputs;

    var skewData = $scope.streamingDag.getReceivedMessages(activeProcessorId).rate;
    var skewDataOption = {
      inject: {
        height: '110px',
        xAxisDataNum: $scope.parallelism
      },
      series: [
        {name: 's1', data: skewData, type: 'bar', clickable: true}
      ],
      xAxisData: function() {
        var array = [];
        for (var i = 0; i < $scope.parallelism; ++i) {
          array.push('t' + i);
        }
        return array;
      }()
    };

    var xAxisDataNum = 25;
    var lineChartOptionBase = {
      inject: {
        height: '108px',
        xAxisDataNum: xAxisDataNum,
        tooltip: {
          formatter: function (params) {
            var s = params[0].name;
            angular.forEach(params, function (param) {
              s += '<br/>' + param.seriesName + ': ' + Number(param.value).toFixed(3);
            });
            return s;
          }
        }
      },
      series: function() {
        var array = [];
        for (var i = 0; i < $scope.parallelism; ++i) {
          array.push({name: 'T'+i, data: [0], scale: true});
        }
        return array;
      }(),
      xAxisData: [0],
      stacked: true
    };

    $scope.chart = {
      receiveMessageRate: {
        options: angular.copy(lineChartOptionBase)
      },
      sendMessageRate: {
        options: angular.copy(lineChartOptionBase)
      },
      processingTime: {
        options: angular.copy(lineChartOptionBase)
      },
      receiveLatency: {
        options: angular.copy(lineChartOptionBase)
      },
      receiveSkew: {
        options: skewDataOption
      }
    };

    var timeoutPromise = $interval(function () {
      if (!$scope.streamingDag.hasMetrics() || activeProcessorId === undefined) {
        return;
      }
      var xLabel = moment().format('HH:mm:ss');
      var receivedMessages = $scope.streamingDag.getReceivedMessages(activeProcessorId);
      var sentMessages = $scope.streamingDag.getSentMessages(activeProcessorId);
      var processingTimes = $scope.streamingDag.getProcessingTime(activeProcessorId);
      var receiveLatencies = $scope.streamingDag.getReceiveLatency(activeProcessorId);

      $scope.chart.receiveMessageRate.data = [{
        x: xLabel,
        y: receivedMessages.rate
      }];
      $scope.chart.sendMessageRate.data = [{
        x: xLabel,
        y: sentMessages.rate
      }];
      $scope.chart.processingTime.data = [{
        x: xLabel,
        y: processingTimes
      }];
      $scope.chart.receiveLatency.data = [{
        x: xLabel,
        y: receiveLatencies
      }];
      $scope.chart.receiveSkew.data = function() {
        var skewData = receivedMessages.rate;
        var array = [];
        for (var i = 0; i < $scope.parallelism; ++i) {
          array.push({x: 't' + i, y: skewData[i]});
        }
        return array;
      }();
    }, conf.updateChartInterval);

    $scope.$on('$destroy', function () {
      $interval.cancel(timeoutPromise);
    });
  }
  ])
;
