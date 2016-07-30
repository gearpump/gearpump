/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
angular.module('dashboard')

  .config(['$stateProvider',
    function ($stateProvider) {
      'use strict';

      $stateProvider
        .state('cluster.master', {
          url: '/master',
          templateUrl: 'views/cluster/master/master.html',
          controller: 'MasterCtrl',
          resolve: {
            master0: ['models', function (models) {
              return models.$get.master();
            }]
          }
        });
    }])

  .controller('MasterCtrl', ['$scope', '$propertyTableBuilder', 'i18n', 'helper', 'models', 'master0',
    function ($scope, $ptb, i18n, helper, models, master0) {
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
      master0.$subscribe($scope, function (master) {
        updateSummaryTable(master);
      });

      // Delegate JvmMetrics directive to manage metrics
      $scope.queryMetricsFnRef = function (all) {
        return all ?
          models.$get.masterHistMetrics() :
          models.$get.masterMetrics($scope.metricsConfig.retainRecentDataIntervalMs);
      };
    }])
;
