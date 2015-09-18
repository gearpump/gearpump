/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
angular.module('dashboard')

  .controller('AppSubmitCtrl', ['$scope', 'restapi',
    function($scope, restapi) {
      'use strict';

      $scope.files = {};
      $scope.names = {};

      $scope.clear = function(name) {
        $scope.files[name] = null;
        $scope.names[name] = ''; // must be '', otherwise MSIE will not response expectedly
      };

      ['jar', 'conf'].forEach(function(name) {
        $scope.clear(name);
        $scope.$watch(name, function(files) {
          if (Array.isArray(files) && files.length) {
            $scope.files[name] = files[0];
            $scope.names[name] = files[0].name;
          }
        });
      });

      $scope.canSubmit = function() {
        return $scope.files.jar && !$scope.uploading;
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