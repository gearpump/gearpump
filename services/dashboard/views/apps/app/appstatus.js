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
    var options = {height: '108px'};
    $scope.charts = [
      {title: 'Input Message Rate (unit: message/s)', options: options, data: []},
      {title: 'Output Message Rate (unit: message/s)', options: options, data: []},
      {title: 'Average Processing Time per Task (Unit: ms)', options: options, data: []},
      {title: 'Average Receive Latency per Task (Unit: ms)', options: options, data: []}
    ];

    var timeoutPromise = $interval(function () {
      if (!$scope.streamingDag || !$scope.streamingDag.hasMetrics()) {
        return;
      }
      $scope.charts[0].data = [$scope.streamingDag.getReceivedMessages().rate];
      $scope.charts[1].data = [$scope.streamingDag.getSentMessages().rate];
      $scope.charts[2].data = [$scope.streamingDag.getProcessingTime()];
      $scope.charts[3].data = [$scope.streamingDag.getReceiveLatency()];
    }, conf.updateChartInterval);

    $scope.$on('$destroy', function () {
      $interval.cancel(timeoutPromise);
    });
  }
  ])
;
