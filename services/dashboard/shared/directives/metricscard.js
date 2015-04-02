/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('directive.metricscard', [])

/**
 * The angular directive represents metrics info in a card.
 */
  .directive('metricsCard', [function () {
    return {
      restrict: 'E',
      scope: true,
      link: function (scope, elems, attrs) {
        scope.title = attrs.mTitle;
        scope.explanation = attrs.mExplanation;
        scope.value = attrs.mValue;
        scope.unit = attrs.mUnit;

        attrs.$observe('mTitle', function (value) {
          scope.title = value;
        });
        attrs.$observe('mExplanation', function (value) {
          scope.explanation = value;
        });
        attrs.$observe('mValue', function (value) {
          scope.value = value;
        });
        attrs.$observe('mUnit', function (value) {
          scope.unit = value;
        });
      },
      template: '<div class="">' +
      '<h6 class="metrics-title">{{title}}' +
      '<span ng-if="explanation" class="glyphicon glyphicon-question-sign metrics-explanation" title="{{explanation}}"></span></h6>' +
      '<h3 class="metrics-value">{{value}} <small>{{unit}}</small></h3>' +
      '</div>'
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
