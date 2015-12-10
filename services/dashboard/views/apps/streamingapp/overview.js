/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .config(['$stateProvider',
    function($stateProvider) {
      'use strict';

      $stateProvider
        .state('streamingapp.overview', {
          url: '', /* default page */
          templateUrl: 'views/apps/streamingapp/overview.html',
          controller: 'StreamingAppOverviewCtrl'
        });
    }])

  .controller('StreamingAppOverviewCtrl', ['$scope', '$propertyTableBuilder', 'helper', 'conf', 'models',
    function($scope, $ptb, helper, conf, models) {
      'use strict';

      $scope.appSummary = [
        $ptb.text('ID').done(),
        $ptb.text('Actor Path').done(),
        $ptb.datetime('Start Time').done(),
        $ptb.text('User').done(),
        $ptb.button('Quick Links').done()
      ];

      function updateSummaryTable(app) {
        $ptb.$update($scope.appSummary, [
          app.appId,
          app.actorPath,
          app.startTime,
          app.user,
          [
            {href: app.configLink, target: '_blank', text: 'Config', class: 'btn-xs'},
            helper.withClickToCopy({text: 'Home Dir.', class: 'btn-xs'}, app.homeDirectory),
            helper.withClickToCopy({text: 'Log Dir.', class: 'btn-xs'}, app.logFile)
          ]
        ]);
      }

      $scope.$watch('app', function(app) {
        updateSummaryTable(app);
      });

      $scope.$on('$destroy', function() {
        $scope.destroyed = true;
      });
      models.$subscribe($scope,
        function() {
          return models.$get.appHistoricalMetrics($scope.app.appId,
            conf.metricsChartSamplingRate, conf.metricsChartDataCount);
        },
        function(historicalMetrics0) {
          $scope.historicalMetrics = historicalMetrics0.$data();
          historicalMetrics0.$subscribe($scope, function(metrics) {
            $scope.historicalMetrics = metrics;
          });
        });
    }])
;