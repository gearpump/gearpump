/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('dashboard.streamingservice', [])

  .factory('StreamingService', ['$http', '$timeout', 'conf', function ($http, $timeout, conf) {
    var webSocketUri;
    $http.get(conf.restapiRoot + '/websocket/url')
      .success(function (data) {
        webSocketUri = data.url;
      });

    return {
      subscribe: function (request, scope, onMessage) {
        scope.$on('$destroy', function () {
          if (ws) {
            ws.close();
          }
        });

        var ws = new WebSocket(webSocketUri);
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