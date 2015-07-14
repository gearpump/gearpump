/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.apps')

  .controller('AppSubmitCtrl', ['$scope', 'restapi',
    function($scope, restapi) {
      $scope.files = {};
      $scope.filenames = {};

      ['jar', 'conf'].forEach(function(name) {
        $scope.files[name] = null;
        $scope.filenames[name] = null;
        $scope.$watch(name, function(val) {
          if (val != null && val.length) {
            $scope.files[name] = val[0];
            $scope.filenames[name] = val[0].name;
          }
        });
      })

      $scope.canSubmit = function() {
        return $scope.files.jar && !$scope.uploading;
      };

      $scope.clear = function(name) {
        $scope.files[name] = null;
        $scope.filenames[name] = null;
      };

      $scope.submit = function() {
        var files = [$scope.files.jar];
        var fileFormNames = ['jar'];
        if ($scope.files.conf) {
          files.push($scope.files.conf);
          fileFormNames.push('conf');
        }
        $scope.uploading = true;
        restapi.submitUserApp(files, fileFormNames, $scope.extraArgs, function(response) {
          $scope.shouldNoticeSubmitFailed = !response.success;
          $scope.uploading = false;
          if (response.success) {
            $scope.$hide();
          }
        });
      };
    }])
;