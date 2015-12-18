/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard', [
  'ngAnimate',
  'ngSanitize',
  'mgcrea.ngStrap',
  'ui.router',
  'ui.select',
  'cfp.loadingBarInterceptor',
  'ngFileUpload',
  'dashing'
])

  // configure routes
  .config(['$stateProvider', '$urlRouterProvider',
    function($stateProvider, $urlRouterProvider) {
      'use strict';

      $urlRouterProvider
        .when('', '/')
        .when('/', '/cluster')
        .when('/cluster', '/cluster/master')/*
        .otherwise(function($injector, $location) {
          // redirects to parent state (recursively)
          var parentUrl = _.initial($location.path().split('/')).join('/');
          $location.path(parentUrl);
        })*/;

      $stateProvider
        .state('cluster', {
          abstract: true, // todo: we have a sidebar for cluster only
          url: '/cluster',
          templateUrl: 'views/cluster/overview.html'
        });
      // Please check every controller for corresponding state definition
    }])

  // configure loading bar effect
  .config(['cfpLoadingBarProvider', function(cfpLoadingBarProvider) {
    'use strict';

    cfpLoadingBarProvider.includeSpinner = false;
    cfpLoadingBarProvider.latencyThreshold = 1000;
  }])

  // configure angular-strap
  .config(['$tooltipProvider', function($tooltipProvider) {
    'use strict';

    angular.extend($tooltipProvider.defaults, {
      html: true
    });
  }])

  // disable caching for ajax calls to make MSIE happy
  .config(['$httpProvider', function($httpProvider) {
    'use strict';

    $httpProvider.defaults.headers.get = angular.merge({
      'Cache-Control': 'no-cache',
      'Pragma': 'no-cache'
    }, $httpProvider.defaults.headers.get);
  }])

  // constants
  .constant('conf', {
    restapiProtocol: 'v1.0',
    restapiRoot: location.origin + location.pathname,
    restapiQueryInterval: 2 * 1000, // 2 seconds
    restapiQueryTimeout: 30 * 1000, // 30 seconds
    metricsDataPointsP5M: 5 * 4, // 1 point per 15 seconds
    metricsDataPointsP48H: 48 // 1 point per hour
  })
;