/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('dashboard.streamingservice', [])

  .factory('StreamingService', ['conf', '$timeout', function (conf, $timeout) {
    return {
      subscribe: function (request, scope, onMessage) {
        scope.$on('$destroy', function () {
          ws.close();
        });

        var ws = new WebSocket(conf.webSocketUri);
        ws.onmessage = onMessage;

        var sendOrRetry = function () {
          if (ws.readyState === 1) {
            ws.send(request);
          } else {
            $timeout(sendOrRetry, conf.webSocketSendTimeout);
          }
        };
        sendOrRetry();
      }
    };
  }])
;