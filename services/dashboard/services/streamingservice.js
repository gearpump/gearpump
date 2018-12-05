/*
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
