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

package org.apache.gearpump.cluster.master

import akka.actor._
import akka.contrib.datareplication.{LWWMap}
import akka.contrib.datareplication.Replicator._
import akka.pattern.ask

import scala.concurrent.Future

class MasterHAService extends Actor with Stash with ClusterReplication {
  import MasterHAService._

  def receive: Receive = stateService orElse stateChangeListener

  override def preStart(): Unit = {
    replicator ! Subscribe(STATE, self)
  }

  override def postStop(): Unit = {
    replicator ! Unsubscribe(STATE, self)
  }

  def stateService: Receive = {
    case GetMasterState =>
      val client = sender

      (replicator ? new Get(STATE, ReadFrom(readQuorum), TIMEOUT, None))
        .asInstanceOf[Future[GetResponse]].map { response =>
        response match {
          case GetSuccess(_, replicatedState: LWWMap, _) =>
            val maxId = replicatedState.get(MaxAppId).asInstanceOf[Option[Int]].getOrElse(0)
            val state = Set.apply(replicatedState.entries.filter(_._1 != MaxAppId).values.toSeq.asInstanceOf[Seq[ApplicationState]] : _*)
            client ! MasterState(maxId, state)
            LOG.info(s"Successfully retrived replicated master state")
          case x: NotFound =>
            LOG.info(s"cannot find any master state")
            client ! MasterState(0, Set.empty[ApplicationState])
          case x: Any =>
            LOG.info(s"Failed to get replicated master state...${x.getClass.getName}")
            client ! GetMasterStateFailed(new Exception(x.getClass.getName))
        }
      }
    case UpdateMasterState(state) =>
      val client = sender
      (replicator ? Update(STATE, LWWMap(), WriteTo(writeQuorum), TIMEOUT){map =>
        val maxId = Math.max(map.get(MaxAppId).asInstanceOf[Option[Int]].getOrElse(0), state.appId)
        map + (state.appId.toString -> state) + (MaxAppId -> maxId)
      }).asInstanceOf[Future[UpdateResponse]].map { case response =>
        response match {
          case UpdateSuccess(key, _) =>
            client ! UpdateMasterStateSuccess
          case fail: UpdateFailure =>
            client ! UpdateMasterStateFailed(new Exception(fail.getClass.getName))
        }
      }

    case DeleteMasterState(appId) =>
      val client = sender
      (replicator ? Update(STATE, LWWMap(), WriteTo(writeQuorum), TIMEOUT)(_ - appId.toString
      )).asInstanceOf[Future[UpdateResponse]].map { case response =>
        response match {
          case UpdateSuccess(key, _) =>
            client ! DeleteMasterStateSuccess
          case fail: UpdateFailure =>
            client ! DeleteMasterStateFailed(new Exception(fail.getClass.getName))
        }
      }
  }
}

object MasterHAService {

/**
 * Master state related
 */
  case object GetMasterState

  trait GetMasterStateResult

  case class MasterState(maxId: Int, state: Set[ApplicationState]) extends GetMasterStateResult

  case class GetMasterStateFailed(ex: Throwable) extends GetMasterStateResult

  case class UpdateMasterState(state: ApplicationState)

  trait UpdateMasterStateResult

  case object UpdateMasterStateSuccess extends UpdateMasterStateResult

  case class UpdateMasterStateFailed(ex: Throwable) extends UpdateMasterStateResult

  case class DeleteMasterState(appId: Int)

  trait DeleteMasterStateResult

  case object DeleteMasterStateSuccess extends DeleteMasterStateResult

  case class DeleteMasterStateFailed(ex: Throwable) extends DeleteMasterStateResult


  def props = Props(new MasterHAService)

  val MaxAppId = "maxAppId"
}
