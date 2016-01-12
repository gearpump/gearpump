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
  'dashing',
  'io.gearpump.models'
])

  // configure routes
  .config(['$stateProvider', '$urlRouterProvider',
    function($stateProvider, $urlRouterProvider) {
      'use strict';

      var landingPageUrl = '/cluster/master';
      $urlRouterProvider
        .when('', '/')
        .when('/', '/cluster')
        .when('/cluster', landingPageUrl)
        .otherwise(function($injector, $location) {
          var currentPath = $location.path();
          if (currentPath !== landingPageUrl) {
            // redirects to parent state (recursively)
            var parentUrl = _.initial(currentPath.split('/')).join('/');
            if (parentUrl !== landingPageUrl) {
              $location.path(parentUrl);
              return;
            }
          }
          $location.path('/404');
        });

      $stateProvider
        .state('404', {
          url: '/404',
          templateUrl: 'views/landing/404.html'
        })
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

  // disable logging for production
  .config(['$compileProvider', function ($compileProvider) {
    'use strict';

    $compileProvider.debugInfoEnabled(false);
  }])

  // constants
  .constant('conf', {
    restapiProtocol: 'v1.0',
    restapiRoot: location.origin + location.pathname,
    restapiQueryInterval: 3 * 1000, // in milliseconds
    restapiQueryTimeout: 30 * 1000, // in milliseconds
    restapiTaskLevelMetricsQueryLimit: 100
  })
;