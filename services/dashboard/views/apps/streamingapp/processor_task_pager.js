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

  .directive('processorTaskPager', function () {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'views/apps/streamingapp/processor_task_pager.html',
      replace: true,
      scope: {
        range: '=',
        count: '=',
        queryLimit: '='
      },
      link: function (scope) {
        'use strict';

        var pages = Math.ceil(scope.count / scope.queryLimit);
        var pageIndex = 0;

        scope.switchTasks = function (step) {
          if (pageIndex + step >= 0 && pageIndex + step < pages) {
            pageIndex += step;
            var start = pageIndex * scope.queryLimit;
            scope.range = {
              start: start,
              stop: Math.min(scope.count, start + scope.queryLimit) - 1
            };
          }
        };

        scope.switchTasks(0);
      }
    };
  })
;
