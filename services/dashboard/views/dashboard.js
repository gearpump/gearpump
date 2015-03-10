/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular
  .module('dashboard', [
    'ngRoute',
    'ng-breadcrumbs',
    'smart-table',
    'filter.readable',
    'dashboard.restapi',
    'dashboard.streamingservice',
    'dashboard.cluster',
    'dashboard.apps',
    'dashboard.apps.appmaster'
  ])

  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider
      // TODO: we need a real landing page
      .when('/', {redirectTo: '/cluster', label: 'Home'})
      .otherwise({redirectTo: '/'});
  }])

  .constant('conf', {
    debug: true,
    restapiAutoRefreshInterval: 2000,
    restapiRoot: '',
    webSocketUri: 'ws://' + document.location.hostname + ':8091/',
    webSocketSendTimeout: 500
  })

  .controller('DashboardCtrl', ['$scope', '$location', 'breadcrumbs', function ($scope, $location, breadcrumbs) {
    $scope.breadcrumbs = breadcrumbs;
    $scope.links = [
      {label: 'Cluster', url: '#/cluster', iconClass: 'glyphicon glyphicon-th-large'},
      {label: 'Applications', url: '#/apps', iconClass: 'glyphicon glyphicon-tasks'}
    ];
    $scope.navClass = function (url) {
      var path = url.substring(1); // without the leading hash prefix char
      return $location.path().indexOf(path) === 0;
    };
  }])
;
