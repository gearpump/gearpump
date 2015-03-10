/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.apps', [])

  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider
      .when('/apps', {
        label: 'Applications',
        templateUrl: 'views/apps/apps.html',
        controller: 'AppsCtrl'
      });
  }])

  .controller('AppsCtrl', ['$scope', '$location', 'restapi', function ($scope, $location, restapi) {
    $scope.view = function (id) {
      $location.path("/apps/app/" + id);
    };
    $scope.kill = function (id) {
      restapi.kill(id);
    };

    $scope.apps = [];
    restapi.subscribe('/appmasters', $scope,
      function (data) {
        var masters = data.appMasters;
        $scope.apps = masters.map(function (app) {
          return {
            active: app.status === 'active',
            appMasterPath: app.appMasterPath,
            finishTime: stringToDateTime(app.finishTime),
            id: app.appId,
            name: app.appName,
            startTime: stringToDateTime(app.startTime),
            submissionTime: stringToDateTime(app.submissionTime),
            status: app.status,
            user: app.user,
            workerPath: app.workerPath
          };
        });
      },
      function (reason, code) {
      });

    function stringToDateTime(s) {
      return s ? moment(Number(s)).format('YYYY/MM/DD HH:mm:ss') : '-';
    }
  }])
;