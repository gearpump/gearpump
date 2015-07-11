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

  .controller('AppsCtrl', ['$scope', '$location', 'restapi', 'util', function ($scope, $location, restapi, util) {
    $scope.operationDenied = {};
    $scope.view = function (id) {
      $location.path("/apps/app/" + id);
    };
    $scope.restart = function (id) {
      $scope.operationDenied[id] = true;
      restapi.restartAppAsync(id).finally(function() {
        delete $scope.operationDenied[id];
      });
    };
    $scope.kill = function (id) {
      restapi.killApp(id);
    };

    $scope.$watch('files', function(files) {
      if (files != null && files.length) {
        $scope.uploading = true;
        restapi.submitUserApp(files[0], function(response) {
          $scope.shouldNoticeUploadFailed = !response.success;
          $scope.uploading = false;
        });
      }
    });

    restapi.subscribe('/master/applist', $scope,
      function (data) {
        // TODO: Serde AppMastersData (#458)
        var masters = data.appMasters;
        $scope.apps = masters.map(function (app) {
          return {
            active: app.status === 'active',
            appMasterPath: app.appMasterPath,
            finishTime: util.stringToDateTime(app.finishTime),
            id: app.appId,
            name: app.appName,
            startTime: util.stringToDateTime(app.startTime),
            submissionTime: util.stringToDateTime(app.submissionTime),
            status: app.status,
            user: app.user,
            workerPath: app.workerPath
          };
        });
      });
  }])
;