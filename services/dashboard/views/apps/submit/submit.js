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

  .controller('AppSubmitCtrl', ['$scope', 'restapi',
    function ($scope, restapi) {
      'use strict';

      $scope.dialogTitle = 'Submit Gearpump Application';
      $scope.confFileSuffix = '.conf';

      var submitFn = restapi.submitUserApp;
      if ($scope.isStormApp) {
        $scope.dialogTitle = 'Submit Storm Application';
        $scope.confFileSuffix = '.yaml';
        submitFn = restapi.submitStormApp;
      }

      $scope.canSubmit = function () {
        return $scope.jar && !$scope.uploading;
      };

      $scope.submit = function () {
        var files = [$scope.jar];
        var fileFormNames = ['jar'];
        if ($scope.conf) {
          files.push($scope.conf);
          fileFormNames.push('configfile');
        }
        $scope.uploading = true;
        submitFn(files, fileFormNames, $scope.executorNum, $scope.launchArgs, function (response) {
          $scope.shouldNoticeSubmitFailed = !response.success;
          $scope.uploading = false;
          if (response.success) {
            $scope.$hide();
          } else {
            $scope.error = response.error;
            $scope.hasStackTrace = response.stackTrace.length > 0;
            $scope.showErrorInNewWin = function () {
              if ($scope.hasStackTrace) {
                var popup = window.open('', 'Error Log');
                var html = [$scope.error].concat(response.stackTrace).join('\n');
                popup.document.open();
                popup.document.write('<pre>' + html + '</pre>');
                popup.document.close();
              }
            }
          }
        });
      };
    }])
;
