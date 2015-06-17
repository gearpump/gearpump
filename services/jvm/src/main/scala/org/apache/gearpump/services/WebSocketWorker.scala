/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.services

import akka.actor._
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterMetricsRequest, StreamingType}
import org.apache.gearpump.shared.Messages.MetricType
import org.apache.gearpump.util.LogUtil
import spray.can.websocket
import spray.can.websocket.FrameCommandFailed
import spray.can.websocket.frame.{BinaryFrame, TextFrame}
import spray.http.HttpRequest
import spray.routing.HttpServiceActor

class WebSocketWorker(val serverConnection: ActorRef, val master: ActorRef) extends HttpServiceActor with websocket.WebSocketServerWorker {
  import upickle._
  final case class Push(msg: String)
  private val LOG = LogUtil.getLogger(getClass)
  var client: ActorRef = _

  override def receive = handshaking orElse businessLogic orElse closeLogic

  def businessLogic: Receive = {
    case x:TextFrame =>
      LOG.info(s"Got TextFrame ${x.payload.utf8String}")
      val req = read[StreamingType](x.payload.utf8String)
      req match {
        case appMasterMetricsRequest: AppMasterMetricsRequest =>
          master ! appMasterMetricsRequest
          client = sender()
        case _ =>
          LOG.error("unknown type")
      }
    case x:BinaryFrame =>
      LOG.info(s"Got BinaryFrame ${x.payload}")

    case metrics: MetricType =>
      LOG.debug("writing metrics")
      client ! TextFrame(write(metrics.json))

    case Push(msg) => send(TextFrame(msg))

    case x: FrameCommandFailed =>
      LOG.error("frame command failed", x)

    case x: HttpRequest => // do something
  }
}

object WebSocketWorker {
  def apply(serverConnection: ActorRef, master: ActorRef)(implicit context: ActorContext): ActorRef = {
    context.actorOf(Props(classOf[WebSocketWorker], serverConnection, master))
  }
}
