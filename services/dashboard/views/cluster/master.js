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
    restapi.subscribe('/master', $scope,
      function (data) {
        // TODO: Serde MasterData (#458)
        var desc = data.masterDescription;
        var condition = desc.masterStatus === 'synced' ? 'good' : 'concern';
        $scope.masterProps = [
          {name: 'Leader', value: desc.leader.join(':')},
          {name: 'Cluster Members', values: desc.cluster.map(function(item) {return item.join(':');})},
          {name: 'Status', value: {text: desc.masterStatus, condition: condition}, renderer: 'State'},
          {name: 'Up Time', value: desc.aliveFor, renderer: 'Duration'}
        ];

        $scope.quickLinks = [
          {name: 'Configuration', value: {href: restapi.masterConfigLink(), text: 'Download'}, renderer: 'Link'},
          {name: 'Home Dir.', value: desc.homeDirectory},
          {name: 'Log Dir.', value: desc.logFile},
          {name: 'JAR Store Dir.', value: desc.jarStore}
        ];
      }
    );
  }])
;