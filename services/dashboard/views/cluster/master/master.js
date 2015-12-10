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
            metrics0: ['models', function(models) {
              return models.$get.masterMetrics();
            }],
            historicalMetrics0: ['models', 'conf', function(models, conf) {
              return models.$get.masterHistoricalMetrics(
                conf.metricsChartSamplingRate, conf.metricsChartDataCount);
            }]
          }
        });
    }])

  .controller('MasterCtrl', ['$scope', '$propertyTableBuilder',
    'helper', 'master0', 'metrics0', 'historicalMetrics0',
    function($scope, $ptb, helper, master0, metrics0, historicalMetrics0) {
      'use strict';

      $scope.masterInfoTable = [
        $ptb.text('JVM Info').help('Format: PID@hostname').done(),
        $ptb.text('Leader').done(),
        $ptb.text('Master Members').done(),
        $ptb.tag('Status').done(),
        $ptb.duration('Up Time').done(),
        $ptb.button('Quick Links').values([
          {href: master0.configLink, target: '_blank', text: 'Config', class: 'btn-xs'},
          helper.withClickToCopy({text: 'Home Dir.', class: 'btn-xs'}, master0.homeDirectory),
          helper.withClickToCopy({text: 'Log Dir.', class: 'btn-xs'}, master0.logFile),
          helper.withClickToCopy({text: 'Jar Store', class: 'btn-xs'}, master0.jarStore)
        ]).done()
      ];

      function updateSummaryTable(master) {
        $ptb.$update($scope.masterInfoTable, [
          master.jvmName,
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

      // JvmMetricsChartsCtrl will watch `$scope.metrics` and `$scope.historicalMetrics`.
      $scope.metrics = metrics0.$data();
      metrics0.$subscribe($scope, function(metrics) {
        $scope.metrics = metrics;
      });
      $scope.historicalMetrics = historicalMetrics0.$data();
      historicalMetrics0.$subscribe($scope, function(metrics) {
        $scope.historicalMetrics = metrics;
      });
    }])
;