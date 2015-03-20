/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard.apps.appmaster')

  .controller('AppMetricsCtrl', ['$scope', function ($scope) {
  }])

  .filter('lpart', function () {
    return function (name) {
      var parts = name.split('.');
      return parts[parts.length - 1];
    };
  })
;
