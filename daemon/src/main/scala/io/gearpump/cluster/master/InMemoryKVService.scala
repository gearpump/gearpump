/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package io.gearpump.cluster.master

import akka.actor._
import akka.contrib.datareplication.LWWMap
import akka.contrib.datareplication.Replicator._
import akka.pattern.ask

import scala.concurrent.Future

class InMemoryKVService extends Actor with Stash with ClusterReplication {
  import InMemoryKVService._

  def receive : Receive = kvService orElse stateChangeListener

  override def preStart(): Unit = {
    replicator ! Subscribe(STATE, self)
  }

  override def postStop(): Unit = {
    replicator ! Unsubscribe(STATE, self)
  }
  def kvService : Receive = {
    case GetKV(group: String, key : String) =>
      val client = sender
      (replicator ? new Get(KVService + group, ReadFrom(readQuorum), TIMEOUT, None)).asInstanceOf[Future[GetResponse]].map {
        case GetSuccess(_, appData: LWWMap, _) =>
          LOG.info(s"Successfully retrived key: $key")
          client ! GetKVSuccess(key, appData.get(key).orNull)
        case x: NotFound =>
          LOG.info(s"We cannot find key $key")
          client ! GetKVSuccess(key, null)
        case x : GetFailure =>
          LOG.error(s"Failed to get application $key data, the request key is $key")
          client ! GetKVFailed(new Exception(x.getClass.getName))
        case GetSuccess(_, x, _) =>
          LOG.error(s"Got unexpected response when get key $key, the response is $x")
          client ! GetKVFailed(new Exception(x.getClass.getName))
      }

    case PutKV(group: String, key : String, value : Any) =>
      val client = sender

      val update = Update(KVService + group, LWWMap(),
        WriteTo(writeQuorum), TIMEOUT) {map =>
        map + (key -> value)
      }

      val putFuture = (replicator ? update).asInstanceOf[Future[UpdateResponse]]

      putFuture.map {
        case UpdateSuccess(key, _) =>
          client ! PutKVSuccess
        case fail: UpdateFailure =>
          client ! PutKVFailed(new Exception(fail.getClass.getName))
      }
    case DeleteKVGroup(group: String) =>
      val client = sender
      replicator ? Update(KVService + group, LWWMap(), WriteTo(writeQuorum), TIMEOUT)( _ => LWWMap())
  }
}

object InMemoryKVService {
  /**
   * KV Service related
   */
  case class GetKV(group: String, key: String)

  trait GetKVResult

  case class GetKVSuccess(key: String, value: Any) extends GetKVResult

  case class GetKVFailed(ex: Throwable) extends GetKVResult

  case class PutKV(group: String, key: String, value: Any)

  case class DeleteKVGroup(group: String)

  trait PutKVResult

  case object PutKVSuccess extends PutKVResult

  case class PutKVFailed(ex: Throwable) extends PutKVResult
}
