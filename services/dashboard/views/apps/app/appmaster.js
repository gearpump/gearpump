/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.apps.appmaster', ['directive.visdag', 'dashboard.streamingdag'])

  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider
      .when('/apps/app/:id', {
        label: 'Application ',
        templateUrl: 'views/apps/app/appmaster.html',
        controller: 'AppMasterCtrl'
      });
  }])

  .controller('AppMasterCtrl',
  ['$scope', '$routeParams', 'breadcrumbs', 'restapi', 'StreamingService', 'visdagUtil', 'StreamingDag',
    function ($scope, $routeParams, breadcrumbs, restapi, StreamingService, visdagUtil, StreamingDag) {
      $scope.app = {id: $routeParams.id};
      breadcrumbs.options = {'Application ': 'Application ' + $scope.app.id};

      $scope.streamingDag = null;
      restapi.subscribe('/appmaster/' + $scope.app.id + '?detail=true', $scope,
        function (data) {
          // TODO: inactive application does not have this key
          if (data.hasOwnProperty('appName')) {
            $scope.app = {
              actorPath: data.actorPath,
              duration: data.clock,
              executors: data.executors,
              id: data.appId,
              name: data.appName
            };
          }

          // TODO: distributed shell does not have these two keys
          if (data.hasOwnProperty('dag') && data.hasOwnProperty('processors')) {
            if ($scope.streamingDag === null) {
              $scope.streamingDag = new StreamingDag($scope.app.id, data.processors,
                data.processorLevels, data.dag.edges);
              $scope.dag = {
                options: visdagUtil.newOptions(),
                data: visdagUtil.newData()
              };
            }
            restapi.metrics($scope.app.id)
              .success(function (data) {
                $scope.streamingDag.updateMetricsArray(data.metrics);
              })
              .finally(function () {
                $scope.streamingDag.updateVisGraphNodes(
                  $scope.dag.data.nodes, [2, 16]);
                $scope.streamingDag.updateVisGraphEdges(
                  $scope.dag.data.edges, [0.5, 4], [0.5, 0.1]);
              });
          }
        },
        function (reason, code) {
        });

      var request = JSON.stringify(
        ["org.apache.gearpump.cluster.MasterToAppMaster.AppMasterMetricsRequest",
          {appId: parseInt($scope.app.id)}
        ]);
      StreamingService.subscribe(request, $scope, function (event) {
        var obj = angular.fromJson(event.data);
        $scope.streamingDag.updateMetrics(obj[0], obj[1]);
      });
    }])

  .factory('visdagUtil', function () {
    var fontFace = "'lato', 'helvetica neue', 'segoe ui', arial";
    return {
      newOptions: function () {
        return {
          width: '100%',
          height: '100%',
          hierarchicalLayout: {
            layout: 'direction',
            direction: "UD"
          },
          stabilize: true /* stabilize positions before displaying */,
          freezeForStabilization: true,
          nodes: {
            shape: 'dot',
            fontSize: 12,
            fontFace: fontFace,
            fontStrokeColor: '#fff',
            fontStrokeWidth: 2
          },
          edges: {
            style: 'arrow',
            labelAlignment: 'line-center',
            fontSize: 11,
            fontFace: fontFace
          }
        };
      },

      newData: function () {
        return {
          nodes: new vis.DataSet(),
          edges: new vis.DataSet()
        };
      }
    };
  })
;