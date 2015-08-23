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
              return models.$get.workerMetrics($stateParams.workerId, /*all=*/true);
            }],
            apps0: ['models', function(models) {
              return models.$get.apps();
            }]
          }
        });
    }])

  .controller('WorkerCtrl', ['$scope', '$propertyTableBuilder', '$sortableTableBuilder',
    'worker0', 'metrics0', 'apps0', 'locator', 'conf',
    function($scope, $ptb, $stb, worker0, metrics0, apps0, locator, conf) {
      'use strict';

      $scope.overviewTable = [
        $ptb.text('ID').done(),
        $ptb.text('Location').done(),
        $ptb.text('Actor Path').done(),
        $ptb.number('Slots Capacity').done(),
        $ptb.button('Quick Links').values([
          {href: worker0.configLink, text: 'Config', class: 'btn-xs'},
          {tooltip: worker0.homeDirectory, text: 'Home Dir.', class: 'btn-xs'},
          {tooltip: worker0.logFile, text: 'Log Dir.', class: 'btn-xs'}
        ]).done()
      ];

      $scope.executorsTable = {
        cols: [
          $stb.link('Executor ID').key('executors').canSort().styleClass('col-md-4').done(),
          $stb.link('Application').key('id').canSort().sortDefault().styleClass('col-md-4').done(),
          $stb.number('Slots').key('slots').canSort().styleClass('col-md-4').done()
        ],
        rows: null
      };

      function updateOverviewTable(worker) {
        $ptb.$update($scope.overviewTable, [
          worker.workerId,
          worker.location,
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
        $scope.worker = worker;
        updateOverviewTable(worker);
        updateExecutorsTable();
      });

      apps0.$subscribe($scope, function(apps) {
        $scope.apps = apps;
        updateExecutorsTable();
      });

      // JvmMetricsChartsCtrl will watch `$scope.metrics`
      $scope.metrics = metrics0.$data();
      metrics0.$subscribe($scope, function(metrics) {
        $scope.metrics = metrics;
      });
    }])
;