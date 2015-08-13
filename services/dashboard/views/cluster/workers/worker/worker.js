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
            modelWorker: ['$stateParams', 'models', function($stateParams, models) {
              return models.$get.worker.detail($stateParams.workerId);
            }],
            modelMetrics: ['$stateParams', 'models', function($stateParams, models) {
              return models.$get.worker.metrics($stateParams.workerId, /*all=*/true);
            }],
            modelApps: ['models', function(models) {
              return models.$get.apps();
            }]
          }
        });
    }])

  .controller('WorkerCtrl', ['$scope', '$propertyTableBuilder', '$sortableTableBuilder',
    'modelWorker', 'modelMetrics', 'modelApps', 'locator', 'conf',
    function($scope, $ptb, $stb, modelWorker, modelMetrics, modelApps, locator, conf) {
      'use strict';

      $scope.overviewTable = [
        $ptb.text('ID').done(),
        $ptb.text('Location').done(),
        $ptb.text('Actor Path').done(),
        $ptb.number('Slots Capacity').done(),
        $ptb.button('Quick Links').done()
      ];

      $scope.executorsTable = {
        cols: [
          $stb.link('Executor ID').key('executors').canSort().styleClass('col-md-3').done(),
          $stb.link('Application').key('id').canSort().sortDefault().styleClass('col-md-6').done(),
          $stb.number('Slots').key('slots').canSort().styleClass('col-md-3').done()
        ],
        rows: null
      };

      function updateOverviewTable(worker) {
        angular.merge($scope.overviewTable, [
          {value: worker.workerId},
          {value: worker.location},
          {value: worker.actorPath},
          {value: worker.slots.total},
          {
            values: [
              {href: worker.configLink, text: 'Config', class: 'btn-xs'},
              {tooltip: worker.homeDirectory, text: 'Home Dir.', class: 'btn-xs'},
              {tooltip: worker.logFile, text: 'Log Dir.', class: 'btn-xs'}
            ]
          }
        ]);
      }

      function updateExecutorsTable() {
        $scope.executorsTable.rows = _.map($scope.worker.executors, function(executor) {
          if ($scope.apps.hasOwnProperty(executor.appId)) {
            var app = $scope.apps[executor.appId];
            var executorPageUrl = locator.executor(app.appId, app.type, executor.executorId);
            var executorPath = 'app' + executor.appId + '.executor' + executor.executorId;
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

      function updateMetricsTable(metrics) {
        var array = metrics['memory.total.used'];
        $scope.chartData = _.map(array, function(metric) {
          return {x: metric.time, y: metric.value};
        });
      }

      $scope.worker = modelWorker.$data();
      $scope.metrics = modelMetrics.$data();
      $scope.apps = modelApps.$data();
      $scope.appsCount = Object.keys($scope.apps).length;

      updateOverviewTable($scope.worker);
      updateExecutorsTable();

      modelWorker.$subscribe($scope, function(worker) {
        $scope.worker = worker;
        updateOverviewTable(worker);
        updateExecutorsTable();
      });


      modelApps.$subscribe($scope, function(apps) {
        $scope.apps = apps;
        updateExecutorsTable();
      });

      var timeLabel = moment().format('HH:mm:ss');
      var initialData = _.map(
        $scope.metrics['memory.total.used'],
        function(metric) {
          return {x: metric.time, y: metric.value};
        });
      var sparkLineOptionBase = {
        height: '108px',
        data: initialData,
        maxDataNum: conf.metricsChartDataCount,
        seriesNames: ['T1']
      };

      $scope.chartOptions = angular.merge({
        valueFormatter: function(value) {
          return Number(value).toFixed(0) + ' bytes';
        }
      }, sparkLineOptionBase);

      updateMetricsTable($scope.metrics);
      modelMetrics.$subscribe($scope, function(metrics) {
        $scope.metrics = metrics;
        updateMetricsTable(metrics);
      });
    }])
;