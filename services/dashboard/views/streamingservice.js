/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('dashboard.streamingservice', [])

  .factory('StreamingService', ['conf', function (conf) {
    return {
      subscribe: function (request, scope, onMessage) {
        scope.$on('$destroy', function () {
          ws.close();
        });

        var ws = new WebSocket(conf.webSocketUri);
        ws.onmessage = onMessage;
        if (conf.debug) {
          ws.onopen = function (event) {
            console.log("CONNECTED");
          };
          ws.onclose = function (event) {
            console.log("DISCONNECTED");
          };
          ws.onerror = function (event) {
            console.log("ERROR " + event.data);
          };
        }
        var trySend = function () {
          switch (ws.readyState) {
            case 1:
              if (conf.debug) {
                console.log("SENDING " + request);
              }
              ws.send(request);
              break;
            default:
              if (conf.debug) {
                console.log("connecting, rs=" + ws.readyState);
              }
              setTimeout(trySend, conf.webSocketSendTimeout);
              break;
          }
        };
        trySend();
      }
    };
  }])
;