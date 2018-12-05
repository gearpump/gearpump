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
      $scope.taskConf = processor.taskConf;

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

        //If only change processor's parallelism, inherit old processor's configuration
        var inheritConf = $scope.changeParallelismOnly || false;
        $scope.dag.replaceProcessor(files, fileFormNames, $scope.app.appId, $scope.processorId, newProcessor, inheritConf, function (response) {
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
