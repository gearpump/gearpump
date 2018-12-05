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
angular.module('dashboard')

  .config(['$stateProvider',
    function ($stateProvider) {
      'use strict';

      $stateProvider
        .state('app', {
          abstract: true,
          url: '/apps/app/:appId',
          templateUrl: 'views/apps/app/app.html',
          controller: 'AppCtrl',
          resolve: {
            app0: ['$stateParams', 'models', function ($stateParams, models) {
              return models.$get.app($stateParams.appId);
            }]
          }
        });
    }])

/**
 * This controller is used to obtain app. All nested views will read status from here.
 */
  .controller('AppCtrl', ['$scope', 'app0',
    function ($scope, app0) {
      'use strict';

      $scope.app = app0.$data();
      app0.$subscribe($scope, function (app) {
        $scope.app = app;
      });
    }])
;
