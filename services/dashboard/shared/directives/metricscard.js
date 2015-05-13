/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('directive.metricscard', ['directive.explanationicon'])

/**
 * This widget shows metrics values in a card.
 */
  .directive('metricsCard', [function () {
    return {
      restrict: 'E',
      scope: true,
      link: function (scope, elems, attrs) {
        attrs.$observe('heading', function (value) {
          scope.heading = value;
        });
        attrs.$observe('explanation', function (value) {
          scope.explanation = value;
        });
        attrs.$observe('value', function (value) {
          scope.value = value;
        });
        attrs.$observe('unit', function (value) {
          scope.unit = value;
        });
      },
      template: '<h6><span class="metrics-title" ng-bind="heading"></span>' +
      '<explanation-icon ng-if="explanation" value="{{explanation}}"/></h6>' +
      '<h3 class="metrics-value">{{value}} <small>{{unit}}</small></h3>'
    };
  }])

  .filter('humanize', function () {
    return function humanize(number) {
      if (number < 1000) {
        return number;
      }
      var si = ['K', 'M', 'G', 'T', 'P', 'H'];
      var exp = Math.floor(Math.log(number) / Math.log(1000));
      var result = number / Math.pow(1000, exp);
      result = (result % 1 > (1 / Math.pow(1000, exp - 1))) ? result.toFixed(2) : result.toFixed(0);
      return result + si[exp - 1];
    };
  })
;
