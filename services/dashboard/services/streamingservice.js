/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('dashboard.streamingservice', [])

  .factory('StreamingService', ['$http', '$timeout', 'conf', function ($http, $timeout, conf) {
    return {
      subscribe: function (request, scope, onMessage) {
        $http.get(conf.restapiRoot + 'websocket/url')
          .success(function (data) {
            var ws = new WebSocket(data.url);
            ws.onmessage = onMessage;
            scope.$on('$destroy', function () {
              ws.close();
            });

            var sendOrRetry = function () {
              if (ws.readyState === 1) {
                ws.send(request);
              } else {
                $timeout(sendOrRetry, conf.webSocketSendTimeout);
              }
            };
            sendOrRetry();
          });
      }
    };
  }])
;