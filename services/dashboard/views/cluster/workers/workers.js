/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('dashboard.cluster')

  .config(['$routeProvider',
    function ($routeProvider) {
      $routeProvider
        .when('/cluster/workers', {
          label: 'Workers',
          templateUrl: 'views/cluster/workers/workers.html',
          controller: 'WorkersCtrl'
        });
    }])

  .controller('WorkersCtrl', ['$scope', 'restapi', function ($scope, restapi) {
    $scope.slotsUsageClass = function (usage, good, concern, bad) {
      return usage < 50 ? good : (usage < 75 ? concern : bad);
    };
    $scope.statusClass = function (status, good, bad) {
      return status === 'active' ? good : bad;
    };

    restapi.subscribe('/workers', $scope,
      function (data) {
        $scope.workers = data.map(function (worker) {
          var slotsUsed = worker.totalSlots - worker.availableSlots;
          return {
            actorPath: worker.actorPath,
            aliveFor: worker.aliveFor,
            executors: worker.executors,
            homeDir: worker.homeDirectory,
            id: worker.workerId,
            logDir: worker.logFile,
            slots: {
              usage: worker.totalSlots > 0 ? Math.floor(100 * slotsUsed / worker.totalSlots) : 0,
              used: slotsUsed,
              total: worker.totalSlots
            },
            status: worker.state
          };
        });
      },
      function (reason, code) {
        $scope.workers = null;
      });
  }])
;