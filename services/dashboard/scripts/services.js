/*
 * The MIT License
 *
 * Copyright (c) 2013, Sebastian Sdorra
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
'use strict';

angular.module('app')
.constant('CONF', {WebSocketSendTimeout:500})
.service('StreamingService', function(CONF) {
  var StreamingService = (function(CONF) {
    function StreamingService() {
      this.wsUri = "ws://localhost:8091/";
      this.onOpen = this.onOpen.bind(this);
      this.onClose = this.onClose.bind(this);
      this.onMessage = this.onMessage.bind(this);
      this.onError = this.onError.bind(this);
      this.send = this.send.bind(this);
      this.websocket = new WebSocket(this.wsUri);
      this.websocket.onopen = this.onOpen;
      this.websocket.onclose = this.onClose;
      this.websocket.onmessage = this.onMessage;
      this.websocket.onerror = this.onError;
    }
    StreamingService.prototype.onOpen = function (evt) {
      console.log("CONNECTED");
    }
    StreamingService.prototype.onClose = function (evt) {
      console.log("DISCONNECTED");
    }
    StreamingService.prototype.onMessage = function (evt) {
      console.log("MESSAGE "+evt.data);
    }
    StreamingService.prototype.onError = function (evt) {
      console.log("ERROR "+evt.data);
    }
    StreamingService.prototype.send = function (data) {
      function send() {
        console.log("connected="+self.websocket.readyState);
        switch(self.websocket.readyState) {
          case 1:
            console.log("SENDING "+data);
            self.websocket.send(data);
            break;
          default:
            setTimeout(send, CONF.WebSocketSendTimeout);
            break;
        }
      }
      try {
        var self = this;
        send();
      } catch(e) {
        console.log("Caught "+e.message);
      }
    }
    return StreamingService;
  })(CONF);
  return new StreamingService;
});
