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

  .directive('header', function () {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'views/landing/header.html',
      replace: true,
      scope: {},
      controller: ['$scope', '$cookies', 'restapi', 'conf', function ($scope, $cookies, restapi, conf) {
        $scope.clusterMenuItems = [
          {text: 'Master', pathPatt: 'master', icon: 'fa fa-laptop'},
          {text: 'Workers', pathPatt: 'workers', icon: 'fa fa-server'}
        ];

        $scope.username = $cookies.get('username');
        $scope.userMenuItems = [
          {text: 'Sign Out', href: conf.loginUrl, icon: 'glyphicon glyphicon-off'},
          {isDivider: true},
          {text: 'Documents', href: '//gearpump.apache.org', icon: 'fa fa-book'},
          {text: 'GitHub', href: '//github.com/apache/incubator-gearpump', icon: 'fa fa-github'}
        ];

        $scope.version = 'beta';
        restapi.serviceVersion(function (version) {
          $scope.version = version;
        });
      }]
    };
  })
;
