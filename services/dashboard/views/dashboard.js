/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular
  .module('dashboard', [
    'ngAnimate',
    'ngRoute',
    'ng-breadcrumbs',
    'mgcrea.ngStrap',
    'ui.select',
    'smart-table',
    'directive.echartfactory',
    'directive.echarts',
    'directive.tabset',
    'directive.explanationicon',
    'directive.metricscard',
    'filter.readable',
    'dashboard.restapi',
    'dashboard.streamingservice',
    'dashboard.cluster',
    'dashboard.apps',
    'dashboard.apps.appmaster'
  ])

  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider
      .when('/', {redirectTo: '/cluster', label: 'Home'})
      .otherwise({redirectTo: '/'});
  }])

  .constant('conf', {
    updateChartInterval: 2000,
    updateVisDagInterval: 2000,
    restapiAutoRefreshInterval: 2000,
    restapiRoot: location.pathname + 'api/v1.0',
    webSocketPreferred: false,
    webSocketSendTimeout: 500
  })

  .controller('DashboardCtrl', ['$scope', '$location', 'breadcrumbs', 'restapi', function ($scope, $location, breadcrumbs, restapi) {
    $scope.breadcrumbs = breadcrumbs;
    $scope.links = [
      {label: 'Cluster', url: '#/cluster', iconClass: 'glyphicon glyphicon-th-large'},
      {label: 'Applications', url: '#/apps', iconClass: 'glyphicon glyphicon-tasks'}
    ];
    $scope.navClass = function (url) {
      var path = url.substring(1); // without the leading hash prefix char
      return $location.path().indexOf(path) === 0;
    };
    restapi.repeatHealthCheck($scope, function(data) {
      $scope.version = data;
    });
  }])
;
