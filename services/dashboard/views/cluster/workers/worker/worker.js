/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .config(['$stateProvider',
    function($stateProvider) {
      'use strict';

      $stateProvider
      /**
       * todo: worker page uses full-width layout, but cluster.master and cluster.workers do not. So we use state
       * 'worker' instead of 'cluster.worker' for the time being, until a new layout is discussed.
       */
        .state('worker', {
          url: '/cluster/workers/worker/:workerId',
          templateUrl: 'views/cluster/workers/worker/worker.html',
          controller: 'WorkerCtrl',
          resolve: {
            worker0: ['$stateParams', 'models', function($stateParams, models) {
              return models.$get.worker($stateParams.workerId);
            }],
            metrics0: ['$stateParams', 'models', function($stateParams, models) {
              return models.$get.workerMetrics($stateParams.workerId);
            }],
            historicalMetrics0: ['$stateParams', 'models', 'conf', function($stateParams, models, conf) {
              return models.$get.workerHistoricalMetrics($stateParams.workerId,
                conf.metricsChartSamplingRate, conf.metricsChartDataCount);
            }],
            apps0: ['models', function(models) {
              return models.$get.apps();
            }]
          }
        });
    }])

  .controller('WorkerCtrl', ['$scope', '$propertyTableBuilder', '$sortableTableBuilder',
    'helper', 'worker0', 'metrics0', 'historicalMetrics0', 'apps0', 'locator',
    function($scope, $ptb, $stb, helper, worker0, metrics0, historicalMetrics0, apps0, locator) {
      'use strict';

      $scope.overviewTable = [
        $ptb.text('JVM Info').help('Format: PID@hostname').done(),
        $ptb.text('Actor Path').done(),
        $ptb.number('Slots Capacity').done(),
        $ptb.button('Quick Links').values([
          {href: worker0.configLink, target: '_blank', text: 'Config', class: 'btn-xs'},
          helper.withClickToCopy({text: 'Home Dir.', class: 'btn-xs'}, worker0.homeDirectory),
          helper.withClickToCopy({text: 'Log Dir.', class: 'btn-xs'}, worker0.logFile)
        ]).done()
      ];

      $scope.executorsTable = {
        cols: [
          $stb.indicator().key('status').canSort().styleClass('td-no-padding').done(),
          $stb.link('Executor ID').key('executors').canSort().styleClass('col-md-4').done(),
          $stb.link('Application').key('id').canSort().sortDefault().styleClass('col-md-4').done(),
          $stb.number('Slots').key('slots').canSort().styleClass('col-md-4').done()
        ],
        rows: null
      };

      function updateOverviewTable(worker) {
        $ptb.$update($scope.overviewTable, [
          worker.jvmName,
          worker.actorPath,
          worker.slots.total
          /* placeholder for quick links, but they do not need to be updated */
        ]);
      }

      function updateExecutorsTable() {
        $scope.executorsTable.rows = _.map($scope.worker.executors, function(executor) {
          if ($scope.apps.hasOwnProperty(executor.appId)) {
            var app = $scope.apps[executor.appId];
            var executorPageUrl = locator.executor(app.appId, app.type, executor.executorId);
            var executorPath = 'app' + executor.appId + '.' +
              (executor.executorId === -1 ?
                'appmaster' : 'executor' + executor.executorId);
            return {
              status: {condition: 'good', shape: 'stripe'}, // always be good
              id: {href: app.pageUrl, text: app.appName},
              executors: {href: executorPageUrl, text: executorPath},
              slots: executor.slots
            };
          } else {
            console.warn({message: 'Unknown executor', executor: executor});
          }
        });
      }

      $scope.worker = worker0.$data();
      $scope.apps = apps0.$data();
      $scope.appsCount = Object.keys($scope.apps).length;

      updateOverviewTable($scope.worker);
      updateExecutorsTable();

      worker0.$subscribe($scope, function(worker) {
        updateWorkerDetails(worker);
      }, /*onerror=*/function() {
        // manually reset status fields on an error response
        var worker = angular.copy($scope.worker);
        worker.state = 'terminated';
        worker.isRunning = false;
        _.forEach(worker.executors, function(executor) {
          executor.status = 'terminated';
          executor.isRunning = false;
        });
        updateWorkerDetails(worker);
      });

      function updateWorkerDetails(worker) {
        $scope.worker = worker;
        updateOverviewTable(worker);
        updateExecutorsTable();
      }

      apps0.$subscribe($scope, function(apps) {
        $scope.apps = apps;
        updateExecutorsTable();
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