/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.apps.appmaster')

  .controller('EditDagCtrl', ['$scope', 'restapi',
    function($scope, restapi) {
      $scope.files = {};
      $scope.filenames = {};
      var options = $scope.modifyOptions || {};
      $scope.changeParallelismOnly = options.parallelism;
      var dagData = $scope.streamingDag.getCurrentDag();
      var processor = dagData.processors[$scope.selectedNodeId];
      $scope.processorId = processor.id;
      $scope.taskClass = processor.taskClass;
      $scope.description = processor.description;
      $scope.parallelism = processor.parallelism;

      ['jar'].forEach(function(name) {
        $scope.files[name] = null;
        $scope.filenames[name] = null;
        $scope.$watch(name, function(val) {
          if (val != null && val.length) {
            $scope.files[name] = val[0];
            $scope.filenames[name] = val[0].name;
          }
        });
      })

      $scope.clear = function(name) {
        $scope.files[name] = null;
        $scope.filenames[name] = null;
      };

      $scope.fillDefaultTime = function() {
        if (!$scope.transitTime) {
          $scope.transitTime = moment($scope.app.clock).format('HH:mm:ss');
        }
      };

      $scope.validParallelism = true;
      $scope.$watch('parallelism', function(val) {
        $scope.validParallelism = val && !isNaN(val);
      });

      $scope.validTaskClass = true;
      $scope.$watch('taskClass', function(val) {
        $scope.validTaskClass = val.length > 0 && /^[a-z_-][a-z\.\d_-]*[a-z\d_-]$/i.test(val);
      });

      $scope.canReplace = function() {
        return $scope.validParallelism && $scope.validTaskClass && $scope.isDirty();
      };

      $scope.isDirty = function() {
        // do not require same type!
        return $scope.taskClass != processor.taskClass ||
            $scope.description != processor.description ||
            $scope.parallelism != processor.parallelism ||
            $scope.transitTime || $scope.transitDate;
      };

      $scope.submit = function() {
        var files = [$scope.files.jar];
        var fileFormNames = ['jar'];
        var newProcessor = {
          taskClass: $scope.taskClass,
          description: $scope.description,
          parallelism: $scope.parallelism
        };
        if ($scope.transitTime) {
          var isoDateTimeString = ($scope.transitDate || moment().format('YYYY-MM-DD')) + 'T' + $scope.transitTime;
          var transitUnixTime = moment(isoDateTimeString).valueOf();
          newProcessor.life = {
            birth: transitUnixTime.toString(),
            death: "9223372036854775807" /* Long.max */
          }
        };
        restapi.replaceDagProcessor(files, fileFormNames, $scope.app.id, $scope.processorId, newProcessor, function(response) {
          $scope.shouldNoticeSubmitFailed = !response.success;
          if (response.success) {
            $scope.$hide();
          } else {
            $scope.reason = response.reason;
          }
        });
      }
    }])
;