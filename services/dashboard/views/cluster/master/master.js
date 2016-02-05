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
            }]
          }
        });
    }])

  .controller('MasterCtrl', ['$scope', '$propertyTableBuilder', 'i18n', 'helper', 'models', 'master0',
    function($scope, $ptb, i18n, helper, models, master0) {
      'use strict';

      $scope.whatIsMaster = i18n.terminology.master;
      $scope.masterInfoTable = [
        $ptb.text('JVM Info').done(),
        $ptb.text('Leader').done(),
        $ptb.text('Master Members').done(),
        $ptb.tag('Status').done(),
        $ptb.duration('Uptime').done(),
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

      $scope.metricsConfig = master0.historyMetricsConfig;
      updateSummaryTable(master0);
      master0.$subscribe($scope, function(master) {
        updateSummaryTable(master);
      });

      // Delegate JvmMetrics directive to manage metrics
      $scope.queryMetricsFnRef = function(all) {
        return all ?
          models.$get.masterHistMetrics() :
          models.$get.masterMetrics($scope.metricsConfig.retainRecentDataIntervalMs);
      };
    }])
;