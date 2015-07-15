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

  .controller('AppsCtrl', ['$scope', '$location', '$modal', 'restapi', 'util',
    function ($scope, $location, $modal, restapi, util) {

    var submitWindow = $modal({
      template: "views/apps/submit.html",
      backdrop: 'static',
      keyboard: false /* https://github.com/mgcrea/angular-strap/issues/1779 */,
      show: false
    });

    $scope.openSubmitDialog = function() {
      submitWindow.$promise.then(submitWindow.show);
    };

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
