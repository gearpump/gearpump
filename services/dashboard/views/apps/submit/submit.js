/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
angular.module('dashboard')

  .controller('AppSubmitCtrl', ['$scope', 'restapi',
    function($scope, restapi) {
      'use strict';

      $scope.dialogTitle = 'Submit Gearpump Application';
      $scope.confFileSuffix = '.conf';
      $scope.executorNum = 1;
      var submitFn = restapi.submitUserApp;
      if ($scope.isStormApp) {
        $scope.dialogTitle = 'Submit Storm Application';
        $scope.confFileSuffix = '.yaml';
        submitFn = restapi.submitStormApp;
      }

      $scope.canSubmit = function() {
        return $scope.jar && !$scope.uploading;
      };

      $scope.submit = function() {
        var files = [$scope.jar];
        var fileFormNames = ['jar'];
        if ($scope.conf) {
          files.push($scope.conf);
          fileFormNames.push('conf');
        }
        $scope.uploading = true;
        submitFn(files, fileFormNames, $scope.executorNum, $scope.launchArgs, function(response) {
          $scope.shouldNoticeSubmitFailed = !response.success;
          $scope.uploading = false;
          if (response.success) {
            $scope.$hide();
          } else {
            $scope.error = response.error;
            $scope.hasStackTrace = response.stackTrace.length > 0;
            $scope.showErrorInNewWin = function() {
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