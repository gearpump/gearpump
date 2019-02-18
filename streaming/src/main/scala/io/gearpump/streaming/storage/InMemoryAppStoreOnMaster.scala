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
package io.gearpump.streaming.storage

import akka.actor.ActorRef
import akka.pattern.ask
import io.gearpump.cluster.AppMasterToMaster.{GetAppData, GetAppDataResult, SaveAppData}
import io.gearpump.util.Constants
import scala.concurrent.Future

/**
 * In memory application storage located on master nodes
 */
class InMemoryAppStoreOnMaster(appId: Int, master: ActorRef) extends AppDataStore {
  implicit val timeout = Constants.FUTURE_TIMEOUT
  import scala.concurrent.ExecutionContext.Implicits.global

  override def put(key: String, value: Any): Future[Any] = {
    master.ask(SaveAppData(appId, key, value))
  }

  override def get(key: String): Future[Any] = {
    master.ask(GetAppData(appId, key)).asInstanceOf[Future[GetAppDataResult]].map { result =>
      if (result.key.equals(key)) {
        result.value
      } else {
        null
      }
    }
  }
}
