/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('dashboard.cluster', [])

  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider
      .when('/cluster', {label: 'Cluster', redirectTo: '/cluster/master'})
      .when('/cluster/master', {
        label: 'Master',
        templateUrl: 'views/cluster/master.html',
        controller: 'MasterCtrl'
      });
  }])

  .controller('MasterCtrl', ['$scope', 'restapi', function ($scope, restapi) {
    $scope.statusClass = function (status, good, bad) {
      return status === 'synced' ? good : bad;
    };

    restapi.subscribe('/master', $scope,
      function (data) {
        // TODO: Serde MasterData (#458)
        var desc = data.masterDescription;
        $scope.master = {
          aliveFor: desc.aliveFor,
          clusters: desc.cluster.map(function (item) {
            return item.join(':');
          }),
          configLink: restapi.masterConfigLink(),
          homeDir: desc.homeDirectory,
          leader: desc.leader.join(':'),
          logDir: desc.logFile,
          jarStore: desc.jarStore,
          status: desc.masterStatus
        };
      }
    );
  }])
;