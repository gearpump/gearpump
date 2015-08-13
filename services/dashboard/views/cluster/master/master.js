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
            modelMaster: ['models', function(models) {
              return models.$get.master();
            }]
          }
        });
    }])

  .controller('MasterCtrl', ['$scope', '$propertyTableBuilder', 'modelMaster',
    function($scope, $ptb, modelMaster) {
      'use strict';

      $scope.masterInfoTable = [
        $ptb.text('Leader').help('The central coordinator').done(),
        $ptb.text('Master Members').done(),
        $ptb.tag('Status').done(),
        $ptb.duration('Up Time').done(),
        $ptb.button('Quick Links').done()
      ];

      function updateSummaryTable(master) {
        angular.merge($scope.masterInfoTable, [
          {value: master.leader},
          {values: master.cluster},
          {value: {text: master.masterStatus, condition: master.isHealthy ? 'good' : 'concern'}},
          {value: master.aliveFor},
          {
            values: [
              {href: master.configLink, text: 'Config', class: 'btn-xs'},
              {tooltip: master.homeDirectory, text: 'Home Dir.', class: 'btn-xs'},
              {tooltip: master.logFile, text: 'Log Dir.', class: 'btn-xs'},
              {tooltip: master.jarStore, text: 'Jar Store', class: 'btn-xs'}
            ]
          }
        ]);
      }

      updateSummaryTable(modelMaster);
      modelMaster.$subscribe($scope, function(master) {
        updateSummaryTable(master);
      });
    }])
;