/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .directive('radioGroup', function() {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'views/widgets/radio_group.html',
      replace: true,
      scope: {
        ngModel: '=',
        options: '='
      },
      link: function(scope, elem, attrs) {
        'use strict';

        scope.buttonWidth = attrs.buttonWidth || '96px';
        scope.toggle = function(value) {
          scope.ngModel = value;
        };
      }
    }
  })
;