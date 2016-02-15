/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .directive('metricsPeriodSwitcher', function() {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'widgets/metrics_period_switcher.html',
      scope: {
        pastHours: '=',
        viewCurrent: '='
      },
      link: function(scope) {
        'use strict';

        scope.options = {
          current: 'Current',
          hist: 'Past %d Hours'.replace('%d', scope.pastHours)
        };
        scope.value = scope.viewCurrent ? 'current' : 'hist';
        scope.$watch('value', function(value) {
          scope.viewCurrent = String(value) === 'current';
        });
      }
    }
  })
;