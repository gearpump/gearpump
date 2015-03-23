/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';
angular.module('dashboard')

  .factory('util', function () {
    return {
      /** */
      stringToDateTime: function (s) {
        return s ? moment(Number(s)).format('YYYY/MM/DD HH:mm:ss') : '-';
      }
    };
  })
;
