/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

/** Provides widgets/directive related helper functions */
  .factory('helper', ['$filter', function($filter) {
    'use strict';

    return {
      /* Allows dashing property `<button>` to have a "Copy to clipboard" feature on click. */
      withClickToCopy: function(values, text) {
        return angular.extend(values, {
          tooltip: 'Location: <b>' + text + '</b><div><small>click to copy</small></div>',
          click: function() {
            clipboard.copy(text);
          }
        });
      },
      /* Returns a readable duration component. */
      readableDuration: function(millis) {
        var pieces = $filter('duration')(millis).split(' ');
        return {
          value: Math.max(0, pieces[0]),
          unit: pieces.length > 1 ? pieces[1] : ''
        };
      },
      /* Show metric as integer or with 2 digits after period, if metric value is less than 1. */
      metricValue: function(value) {
        var precision = Math.abs(value) < 1 ? 2 : 0;
        return $filter('number')(value, precision);
      }
    };
  }])

  .filter('metric', ['helper', function(helper) {
    'use strict';

    return function(value) {
      return helper.metricValue(value);
    };
  }])
;