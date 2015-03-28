/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.apps.appmaster')

  .controller('AppStatusCtrl', ['$scope', 'restapi', 'util', function ($scope, restapi, util) {

    restapi.subscribe('/appmaster/' + $scope.app.id, $scope,
      function (data) {
        // TODO: Serde GeneralAppMasterDataDetail (#458)
        if (data.hasOwnProperty('appName')) {
          $scope.summary = [
            {name: 'Name', value: data.appName},
            {name: 'ID', value: data.appId},
            {name: 'Status', value: data.status || 'Unknown',
              clazz: 'label label-' + (data.status === 'active' ? 'success':'default')},
            {name: 'Submission Time', value: util.stringToDateTime(data.submissionTime) || '-'},
            {name: 'Start Time', value: util.stringToDateTime(data.startTime) || '-'},
            {name: 'Stop Time', value: util.stringToDateTime(data.finishTime) || '-'}
          ];
        }
      });
  }])

  .controller('AppSummaryChartsCtrl', ['$scope', '$interval', 'conf', function ($scope, $interval, conf) {
    var chartHeight = '108px';
    $scope.chart = {
      options: {height: chartHeight},
      throughput: [],
      processTime: [],
      receiveLatency: []
    };

    var timeoutPromise = $interval(function () {
      if (!$scope.streamingDag || !$scope.streamingDag.hasMetrics()) {
        return;
      }
      $scope.chart.throughput = [function () {
        var throughput = $scope.streamingDag.getThroughput();
        return throughput.sent + throughput.received;
      }()];
      $scope.chart.processTime = [$scope.streamingDag.getProcessTime()];
      $scope.chart.receiveLatency = [$scope.streamingDag.getReceiveLatency()];
    }, conf.updateChartInterval);

    $scope.$on('$destroy', function () {
      $interval.cancel(timeoutPromise);
    });
  }
  ])
;
