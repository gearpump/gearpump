/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .directive('processorTaskPager', function() {
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
      link: function(scope) {
        'use strict';

        var pages = Math.ceil(scope.count / scope.queryLimit);
        var pageIndex = 0;

        scope.switchTasks = function(step) {
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