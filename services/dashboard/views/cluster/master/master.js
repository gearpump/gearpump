/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .config(['$stateProvider',
    function($stateProvider) {
      'use strict';

      $stateProvider
        .state('cluster.master', {
          url: '/master',
          templateUrl: 'views/cluster/master/master.html',
          controller: 'MasterCtrl',
          resolve: {
            master0: ['models', function(models) {
              return models.$get.master();
            }],
            metrics0: ['$stateParams', 'models', function($stateParams, models) {
              return models.$get.masterMetrics(/*all=*/true);
            }]
          }
        });
    }])

  .controller('MasterCtrl', ['$scope', '$propertyTableBuilder', 'master0', 'metrics0',
    function($scope, $ptb, master0, metrics0) {
      'use strict';

      $scope.masterInfoTable = [
        $ptb.text('Leader').help('The central coordinator').done(),
        $ptb.text('Master Members').done(),
        $ptb.tag('Status').done(),
        $ptb.duration('Up Time').done(),
        $ptb.button('Quick Links').values([
          {href: master0.configLink, text: 'Config', class: 'btn-xs'},
          {tooltip: master0.homeDirectory, text: 'Home Dir.', class: 'btn-xs'},
          {tooltip: master0.logFile, text: 'Log Dir.', class: 'btn-xs'},
          {tooltip: master0.jarStore, text: 'Jar Store', class: 'btn-xs'}
        ]).done()
      ];

      function updateSummaryTable(master) {
        $ptb.$update($scope.masterInfoTable, [
          master.leader,
          master.cluster,
          {text: master.masterStatus, condition: master.isHealthy ? 'good' : 'concern'},
          master.aliveFor
          /* placeholder for quick links, but they do not need to be updated */
        ]);
      }

      updateSummaryTable(master0);
      master0.$subscribe($scope, function(master) {
        updateSummaryTable(master);
      });

      // JvmMetricsChartsCtrl will watch `$scope.metrics`
      $scope.metrics = metrics0.$data();
      metrics0.$subscribe($scope, function(metrics) {
        $scope.metrics = metrics;
      });
    }])
;