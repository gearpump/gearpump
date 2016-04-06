/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
angular.module('dashboard')

  .controller('StreamingAppDagEditCtrl', ['$scope', 'models',
    function ($scope, models) {
      'use strict';

      var options = $scope.modifyOptions || {};
      $scope.changeParallelismOnly = options.parallelism;

      var processor = $scope.dag.getProcessor($scope.activeProcessorId);
      $scope.processorId = processor.id;
      $scope.taskClass = processor.taskClass;
      $scope.description = processor.description;
      $scope.parallelism = processor.parallelism;

      $scope.invalid = {};
      $scope.canReplace = function () {
        return !_.includes($scope.invalid, true) && $scope.isDirty();
      };

      $scope.isDirty = function () {
        // do not require same type!
        return $scope.taskClass != processor.taskClass ||
          $scope.description != processor.description ||
          $scope.parallelism != processor.parallelism;
      };

      $scope.submit = function () {
        var files = [$scope.jar];
        var fileFormNames = ['jar'];
        var newProcessor = {
          taskClass: $scope.taskClass,
          description: $scope.description,
          parallelism: $scope.parallelism
        };

        if (Array.isArray($scope.transitTime) && $scope.transitTime.length === 2) {
          var tuple = [$scope.transitTime[0] || '', $scope.transitTime[1] || ''];
          var format = 'YYYY-MM-DD';
          var timeString = tuple[0].length === format.length ? tuple[0] : moment().format(format);
          if (tuple[1].length === '00:00:00'.length) {
            timeString += 'T' + tuple[1];
          }
          var transitUnixTime = moment(timeString).valueOf();
          newProcessor.life = {
            birth: transitUnixTime.toString(),
            death: models.DAG_DEATH_UNSPECIFIED
          };
        }

        $scope.dag.replaceProcessor(files, fileFormNames, $scope.app.appId, $scope.processorId, newProcessor, function (response) {
          $scope.shouldNoticeSubmitFailed = !response.success;
          if (response.success) {
            $scope.$hide();
          } else {
            $scope.reason = response.reason;
          }
        });
      };
    }])
;