/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .config(['$stateProvider',
    function($stateProvider) {
      'use strict';

      $stateProvider
        .state('streamingapp.executor', {
          url: '/executor/:executorId',
          templateUrl: 'views/apps/streamingapp/executor.html',
          controller: 'StreamingAppExecutorCtrl'
        });
    }])

  .controller('StreamingAppExecutorCtrl', ['$scope', '$stateParams', '$propertyTableBuilder',
    function($scope, $stateParams, $ptb) {
      'use strict';

      $scope.overviewTable = [
        $ptb.text('ID').done(),
        $ptb.text('Actor Path').done(),
        $ptb.text('Worker').done()
      ];


      function updateOverviewTable(executor) {
        angular.merge($scope.overviewTable, [
          {value: executor.executorId},
          {value: executor.executor},
          {value: executor.workerId}
        ]);
      }

      updateOverviewTable($scope.app.executors[$stateParams.executorId]);
    }])
;
