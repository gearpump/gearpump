/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

/** Provides widgets/directive related helper functions */
  .factory('helper', ['$filter', '$echarts', function($filter, $echarts) {
    'use strict';

    return {
      /* Allows dashing property `<button>` to have a "Copy to clipboard" feature on click. */
      withClickToCopy: function(values, text) {
        return angular.extend(values, {
          tooltip: 'Location: <b>' + text + '</b><div><small>click button to copy</small></div>',
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
      /* Return a readable metric value. */
      readableMetricValue: function(value) {
        if (angular.isNumber(value)) {
          var precision = Math.abs(value) < 100 ? 2 : 0;
          return $filter('number')(value, precision);
        }
        return value;
      },
      /* Make metric precision consistent */
      metricRounded: function(value) {
        return _.round(value, 2);
      },
      /* Create a proper chart time label for echart */
      timeToChartTimeLabel: function(time, shortForm) {
        return moment(time).format(shortForm ? 'HH:mm:ss': 'ddd DD, HH:mm');
      },
      /* Return a y-axis label formatter that will not show 0 on y-axis */
      yAxisLabelFormatterWithoutValue0: function(unit) {
        return $echarts.axisLabelFormatter(unit, {0: ''});
      }
    };
  }])

  .filter('metric', ['helper', function(helper) {
    'use strict';

    return function(value) {
      return helper.readableMetricValue(value);
    };
  }])
;