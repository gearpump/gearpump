/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.apps.appmaster')

  .controller('AppProcessorCtrl', ['$scope', function ($scope) {

    function makeArray(size) {
      var array = [];
      for (var i = 0; i < size; ++i) {
        array.push(Math.random(50, 100));
      }
      return array;
    }

    if ($scope.activeProcessorId) {
      var activeProcessorId = $scope.activeProcessorId;
      var processor = $scope.streamingDag.processors[activeProcessorId];
      $scope.taskClass = processor.taskClass;
      $scope.description = processor.description;
      $scope.parallelism = processor.parallelism;
      var connections = $scope.streamingDag.calculateProcessorConnections(activeProcessorId);
      $scope.inputs = connections.inputs;
      $scope.outputs = connections.outputs;


      var xAxisDataNum = $scope.parallelism;
      var skewData = makeArray(xAxisDataNum);
      $scope.opt1 = {
        inject: {
          height: '110px',
          xAxisDataNum: xAxisDataNum,
          tooltip: false,
          xAxis: [{boundaryGap: true}],
          grid: {y: 50, x2: 5}
        },
        series: [
          {name: 's1', data: skewData, type: 'bar', clickable: true}
        ],
        xAxisData: function() {
          var array = [];
          for (var i = 0; i < xAxisDataNum; ++i) {
            array.push('t' + i);
          }
          return array;
        }()
      };
    }
  }])

  .controller('AppTasksChartsCtrl', ['$scope', '$interval', 'conf', function ($scope, $interval, conf) {

    function makeArray(size) {
      var array = [];
      for (var i = 0; i < size; ++i) {
        array.push(Math.random(50, 100));
      }
      return array;
    }

    var xAxisDataNum = 25;
    $scope.options2 = {
      inject: {
        height: '108px',
        xAxisDataNum: xAxisDataNum,
        tooltip: {
          formatter: function (params) {
            var s = params[0].name;
            angular.forEach(params, function (param) {
              s += '<br/>' + param.seriesName + ': ' + Number(param.value).toFixed(0);
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
      xAxisData: [0]
    };

    $scope.chart = {
      receiveMessageRate: {
        options: angular.copy($scope.options2)
      },
      sendMessageRate: {
        options: angular.copy($scope.options2)
      },
      processingTime: {
        options: angular.copy($scope.options2)
      },
      receiveLatency: {
        options: angular.copy($scope.options2)
      }
    };

    var timeoutPromise = $interval(function () {
      var taskId = 0;
      if (!$scope.streamingDag.hasMetrics() || !$scope.activeProcessorId) {
        return;
      }
      var xLabel = moment().format('HH:mm:ss');
      $scope.chart.receiveMessageRate.data = [{
        x: xLabel,
        y: [$scope.streamingDag.getReceivedMessages($scope.activeProcessorId).rate]
      }];
      $scope.chart.sendMessageRate.data = [{
        x: xLabel,
        y: [$scope.streamingDag.getSentMessages($scope.activeProcessorId).rate]
      }];
      $scope.chart.processingTime.data = [{
        x: xLabel,
        y: $scope.streamingDag.getProcessingTime($scope.activeProcessorId)
      }];
      $scope.chart.receiveLatency.data = [{
        x: xLabel,
        y: $scope.streamingDag.getReceiveLatency($scope.activeProcessorId)
      }];
    }, conf.updateChartInterval);

    $scope.$on('$destroy', function () {
      $interval.cancel(timeoutPromise);
    });
  }
  ])
;
