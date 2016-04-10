/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

/** Routing helper */
  .factory('locator', function () {
    'use strict';

    var self = {
      worker: function (workerId) {
        return '#/cluster/workers/worker/' + workerId;
      },
      executor: function (appId, appType, executorId) {
        return self.app(appId, appType) + '/executor/' + executorId;
      },
      app: function (appId, appType) {
        var prefix = appType === 'streaming' ? 'streaming' : '';
        return '#/apps/' + prefix + 'app/' + appId;
      }
    };
    return self;
  })
;
