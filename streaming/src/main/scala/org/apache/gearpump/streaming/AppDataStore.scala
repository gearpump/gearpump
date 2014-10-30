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
package org.apache.gearpump.streaming

import akka.actor.ActorRef
import akka.pattern.ask
import org.apache.gearpump.cluster.AppMasterToMaster.{GetAppData, SaveAppData}
import org.apache.gearpump.cluster.MasterToAppMaster.GetAppDataResult
import org.apache.gearpump.util.Constants

import scala.concurrent._

trait AppDataStore {
  def put(key: String, value: Any): Future[Any]

  def get(key: String) : Future[Any]
}

class RemoteAppDataStore(appId: Int, master: ActorRef) extends AppDataStore {
  private var needToUpdateStartClock = true
  implicit val timeout = Constants.FUTURE_TIMEOUT
  import scala.concurrent.ExecutionContext.Implicits.global

  override def put(key: String, value: Any): Future[Any] = {
    if(needToUpdateStartClock){
      needToUpdateStartClock = false
      master.ask(SaveAppData(appId, key, value)).map { result =>
        needToUpdateStartClock = true
        result
      }
    } else {
      future {
        throw new Exception(s"Update app data $key failed")
      }
    }
  }

  override def get(key: String): Future[Any] = {
    master.ask(GetAppData(appId, key)).asInstanceOf[Future[GetAppDataResult]].map{result =>
      if(result.key.equals(key)) {
        result.value
      } else {
        null
      }
    }
  }
}
