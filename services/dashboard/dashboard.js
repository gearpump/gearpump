/*
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
(function () {

  // rootPath of this site, it has a tailing slash /
  var rootPath = function () {
    var root = location.origin + location.pathname;
    return root.substring(0, root.lastIndexOf("/") + 1);
  }();

  angular.module('dashboard', [
    'ngAnimate',
    'ngSanitize',
    'ngCookies',
    'ngTouch',
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
      function ($stateProvider, $urlRouterProvider) {
        'use strict';

        $urlRouterProvider
          .when('', '/')
          .when('/', '/cluster')
          .when('/cluster', '/cluster/master');

        $stateProvider
          .state('cluster', {
            abstract: true, // todo: we have a sidebar for cluster only
            url: '/cluster',
            templateUrl: 'views/cluster/overview.html'
          });
        // Please check every controller for corresponding state definition
      }])

    // configure loading bar effect
    .config(['cfpLoadingBarProvider', function (cfpLoadingBarProvider) {
      'use strict';

      cfpLoadingBarProvider.includeSpinner = false;
      cfpLoadingBarProvider.latencyThreshold = 1000;
    }])

    // configure angular-strap
    .config(['$tooltipProvider', function ($tooltipProvider) {
      'use strict';

      angular.extend($tooltipProvider.defaults, {
        html: true
      });
    }])

    // configure dashing
    .config(['dashing.i18n', function (i18n) {
      'use strict';

      i18n.confirmationYesButtonText = 'OK';
      i18n.confirmationNoButtonText = 'Cancel';
    }])

    // disable logging for production
    .config(['$compileProvider', function ($compileProvider) {
      'use strict';

      $compileProvider.debugInfoEnabled(false);
    }])

    // constants
    .constant('conf', {
      restapiProtocol: 'v1.0',
      restapiRoot: rootPath,
      restapiQueryInterval: 3 * 1000, // in milliseconds
      restapiQueryTimeout: 30 * 1000, // in milliseconds
      restapiTaskLevelMetricsQueryLimit: 100,
      loginUrl: rootPath + 'login'
    })

    /* add a retry delay for angular-ui-router, when resolving a data is failed */
    .run(['$rootScope', '$state', 'conf', function ($rootScope, $state, conf) {
      'use strict';

      $rootScope.$on('$stateChangeError', function (event, toState) {
        event.preventDefault();
        _.delay($state.go, conf.restapiQueryTimeout, toState);
      });
    }])

    /* enable a health check service */
    .run(['$modal', 'HealthCheckService', 'conf', function ($modal, HealthCheckService, conf) {
      'use strict';

      var dialog = $modal({
        templateUrl: 'views/service_unreachable_notice.html',
        backdrop: 'static',
        show: false
      });

      var showDialogFn = function () {
        dialog.$promise.then(dialog.show);
      };

      var hideDialogFn = function () {
        // simply refresh the page, to make sure page status is fresh
        location.reload();
      };

      HealthCheckService.config(
        conf.restapiRoot + 'version',
        conf.restapiQueryInterval,
        showDialogFn,
        hideDialogFn
      );
      HealthCheckService.checkForever();
    }]);
})();
