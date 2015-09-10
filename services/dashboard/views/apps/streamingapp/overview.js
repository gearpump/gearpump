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
          controller: 'StreamingAppOverviewCtrl',
          resolve: {
            historicalMetrics0: ['$stateParams', 'models', 'conf', function($stateParams, models, conf) {
              return models.$get.appHistoricalMetrics($stateParams.appId,
                conf.metricsChartSamplingRate, conf.metricsChartDataCount);
            }]
          }
        });
    }])

  .controller('StreamingAppOverviewCtrl', ['$scope', '$propertyTableBuilder', 'historicalMetrics0',
    function($scope, $ptb, historicalMetrics0) {
      'use strict';

      $scope.appSummary = [
        $ptb.text('ID').done(),
        $ptb.datetime('Start Time').done(),
        $ptb.text('User').done(),
        $ptb.text('Actor Path').done(),
        $ptb.button('Quick Links').done()
      ];

      function updateSummaryTable(app) {
        $ptb.$update($scope.appSummary, [
          app.appId,
          app.startTime,
          app.user,
          app.actorPath,
          [
            {href: app.configLink, text: 'Config', class: 'btn-xs'},
            {tooltip: app.homeDirectory, text: 'Home Dir.', class: 'btn-xs'},
            {tooltip: app.logFile, text: 'Log Dir.', class: 'btn-xs'}
          ]
        ]);
      }

      $scope.$watch('app', function(app) {
        updateSummaryTable(app);
      });

      $scope.historicalMetrics = historicalMetrics0.$data();
      historicalMetrics0.$subscribe($scope, function(metrics) {
        $scope.historicalMetrics = metrics;
      });
    }])
;