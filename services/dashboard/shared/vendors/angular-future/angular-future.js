/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

(function () {
  'use strict';

  if (!angular) return;

  // Merge is introduced in Angular 1.4
  if (angular.merge === undefined) {
    var isArray = Array.isArray;
    var setHashKey = function (obj, h) {
      if (h) {
        obj.$$hashKey = h;
      }
      else {
        delete obj.$$hashKey;
      }
    };

    var baseExtend = function (dst, objs, deep) {
      var h = dst.$$hashKey;
      for (var i = 0, ii = objs.length; i < ii; ++i) {
        var obj = objs[i];
        if (!angular.isObject(obj) && !angular.isFunction(obj)) continue;
        var keys = Object.keys(obj);
        for (var j = 0, jj = keys.length; j < jj; j++) {
          var key = keys[j];
          var src = obj[key];
          if (deep && angular.isObject(src)) {
            if (!angular.isObject(dst[key])) dst[key] = isArray(src) ? [] : {};
            baseExtend(dst[key], [src], true);
          } else {
            dst[key] = src;
          }
        }
      }
      setHashKey(dst, h);
      return dst;
    };

    angular.merge = function (dst) {
      var slice = [].slice;
      return baseExtend(dst, slice.call(arguments, 1), true);
    };
  }
}());