/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('directive.explanationicon', ['mgcrea.ngStrap.tooltip'])

/**
 * This widget is rendered as an icon. When user mouse over it, it will show an explanation text.
 */
  .directive('explanationIcon', [function () {
    return {
      restrict: 'E',
      link: function (scope, elems, attrs) {
        scope.value = attrs.value;
      },
      template: '<span class="glyphicon glyphicon-question-sign metrics-explanation" ' +
      ' bs-tooltip="value"></span>'
    };
  }])
;
