/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .config(['$stateProvider',
    function($stateProvider) {
      'use strict';

      $stateProvider
        .state('streamingapp.processor', {
          url: '/processor/:processorId',
          templateUrl: 'views/apps/streamingapp/processor.html',
          controller: 'StreamingAppProcessorCtrl'
        });
    }])

  .controller('StreamingAppProcessorCtrl', ['$scope', '$state', '$stateParams', '$propertyTableBuilder',
    function($scope, $state, $stateParams, $ptb) {
      'use strict';

      $scope.processor = $scope.activeProcessor ||
        $scope.dag.processors[$stateParams.processorId];
      if (!$scope.processor) {
        return $state.go('streamingapp.overview', {appId: $stateParams.appId});
      }

      $scope.processorInfoTable = [
        $ptb.text('Task Class').done(),
        $ptb.number('Parallelism').done(),
        $ptb.text('Inputs').done(),
        $ptb.text('Outputs').done()
      ];

      function updateProcessorInfoTable(processor) {
        var connections = $scope.dag.calculateProcessorConnections(processor.id);
        angular.merge($scope.processorInfoTable, [
          {value: _.last(processor.taskClass.split('.'))},
          {value: processor.parallelism},
          {value: connections.inputs + ' processor(s)'},
          {value: connections.outputs + ' processor(s)'}
        ]);

        $scope.tasks = {
          selected: [],
          available: function() {
            var array = [];
            for (var i = 0; i < processor.parallelism; ++i) {
              array.push('T' + i);
            }
            return array;
          }()
        };
      }

      $scope.$watch('processor', function(processor) {
        updateProcessorInfoTable(processor);
      });
/*
      // todo: individual controller for skew chart
      var skewData = $scope.dag.getReceivedMessages($scope.processor.id).rate;
      var skewDataOption = {
        height: '110px',
        maxDataNum: $scope.parallelism,
        initialData: $scope.tasks.available
      };

      if ($scope.parallelism > 20) {
        skewDataOption.dataZoom = {show: true, realtime: true, y: 0, height: 20};
        skewDataOption.grid = {y: 35};
      }

      $scope.receiveSkewChart = {
        options: skewDataOption
      };*/
    }])
;