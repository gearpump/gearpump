/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.apps')

  .controller('AppSubmitCtrl', ['$scope', 'restapi',
    function($scope, restapi) {
      $scope.filename = $scope.fileToUpload.name;
      $scope.filesize = ($scope.fileToUpload.size / 1024).toFixed(0) + ' KB';

      $scope.submit = function(file) {
        $scope.uploading = true;
        restapi.submitUserApp(file, $scope.extraArgs, function(response) {
          $scope.shouldNoticeSubmitFailed = !response.success;
          $scope.uploading = false;
          if (response.success) {
            $scope.$hide();
          }
        });
      };
    }])
;