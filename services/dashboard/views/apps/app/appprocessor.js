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
    $scope.tasks = {
      selected: [],
      available: function () {
        var array = [];
        for (var i = 0; i < $scope.parallelism; ++i) {
          array.push('T' + i);
        }
        return array;
      }()
    };

    var skewData = $scope.streamingDag.getReceivedMessages(activeProcessorId).rate;
    var skewDataOption = {
      inject: {
        height: '110px',
        xAxisDataNum: $scope.parallelism,
        tooltip: {
          formatter: function (params) {
            return '<strong>' + params.name + '</strong><br/>' +
              Number(params.data).toFixed(0) + ' msgs/s';
          }
        }
      },
      series: [
        {name: 's1', data: skewData, type: 'bar', clickable: true}
      ],
      xAxisData: $scope.tasks.available
    };

    if ($scope.parallelism > 20) {
      skewDataOption.inject.dataZoom = {show: true, realtime: true, y: 0, height: 20};
      skewDataOption.inject.grid = {y: 35};
    }

    $scope.receiveSkewChart = {
      options: skewDataOption
    };

    var timeoutPromise = $interval(function () {
      if (!$scope.streamingDag.hasMetrics() || activeProcessorId === undefined) {
        return;
      }
      var receivedMessages = $scope.streamingDag.getReceivedMessages(activeProcessorId);
      $scope.receiveSkewChart.data = function () {
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
  }])

  .controller('AppProcessorChartsCtrl', ['$scope', '$interval', 'conf',
    function ($scope, $interval, conf) {
      $scope.$watchCollection('tasks', function(tasks) {
        if (tasks.selected) {
          var xAxisDataNum = 15;
          var lineChartOptionBase = {
            inject: {
              height: '108px',
              xAxisDataNum: xAxisDataNum,
              tooltip: {
                formatter: function (params) {
                  var s = params[0].name;
                  angular.forEach(params, function (param) {
                    s += '<br/>' + param.seriesName + ': ' + Number(param.value).toFixed(2);
                  });
                  return s;
                }
              }
            },
            series: function () {
              var array = [];
              for (var i = 0; i < $scope.tasks.selected.length; ++i) {
                array.push({name: $scope.tasks.selected[i], data: [0], scale: true});
              }
              return array;
            }(),
            xAxisData: [0],
            stacked: true
          };

          $scope.receiveMessageRateChart = {
            options: angular.copy(lineChartOptionBase)
          };

          $scope.sendMessageRateChart = {
            options: angular.copy(lineChartOptionBase)
          };

          $scope.processingTimeChart = {
            options: angular.copy(lineChartOptionBase)
          };

          $scope.receiveLatencyChart = {
            options: angular.copy(lineChartOptionBase)
          };
        }
      });

      var timeoutPromise = $interval(function () {
        var activeProcessorId = Number($scope.activeProcessorId);
        if (!$scope.streamingDag.hasMetrics() || activeProcessorId === undefined) {
          return;
        }
        var xLabel = moment().format('HH:mm:ss');
        var receivedMessages = $scope.streamingDag.getReceivedMessages(activeProcessorId);
        var sentMessages = $scope.streamingDag.getSentMessages(activeProcessorId);
        var processingTimes = $scope.streamingDag.getProcessingTime(activeProcessorId);
        var receiveLatencies = $scope.streamingDag.getReceiveLatency(activeProcessorId);

        function filterUnselectedTasks(array) {
          var result = [];
          var selectedTasks = $scope.tasks.selected;
          for (var i in selectedTasks) {
            var task = selectedTasks[i];
            var id = Number(task.substr(1));
            result.push(array[id]);
          }
          return result;
        }

        $scope.receiveMessageRateChart.data = [{
          x: xLabel,
          y: filterUnselectedTasks(receivedMessages.rate)
        }];
        $scope.sendMessageRateChart.data = [{
          x: xLabel,
          y: filterUnselectedTasks(sentMessages.rate)
        }];
        $scope.processingTimeChart.data = [{
          x: xLabel,
          y: filterUnselectedTasks(processingTimes)
        }];
        $scope.receiveLatencyChart.data = [{
          x: xLabel,
          y: filterUnselectedTasks(receiveLatencies)
        }];
      }, conf.updateChartInterval);

      $scope.$on('$destroy', function () {
        $interval.cancel(timeoutPromise);
      });
    }])
;
