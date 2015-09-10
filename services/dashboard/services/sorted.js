/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  /** Routing helper */
  .factory('sorted', function() {
    'use strict';

    return {
      byKey: function(object) {
        var result = {};
        _.forEach(Object.keys(object).sort(), function(key) {
          result[key] = object[key];
        });
        return result;
      }
    };
  })
;