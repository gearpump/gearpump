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

import io.gearpump.cluster.{MasterHarness, MiniCluster}
import io.gearpump.streaming.StreamingTestUtil
import io.gearpump.util.Constants
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import scala.concurrent.Await
import scala.concurrent.duration._

class InMemoryAppStoreOnMasterSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  implicit val timeout = Constants.FUTURE_TIMEOUT
  implicit val dispatcher = MasterHarness.cachedPool

  "InMemoryAppStoreOnMaster" should {
    "save and return the data properly" in {
      val appId = 0
      val miniCluster = new MiniCluster
      val master = miniCluster.mockMaster
      StreamingTestUtil.startAppMaster(miniCluster, appId)
      val store = new InMemoryAppStoreOnMaster(appId, master)

      Thread.sleep(500)

      store.put("String_type", "this is a string")
      store.put("Int_type", 1024)
      store.put("Tuple2_type", ("element1", 1024))

      val future1 = store.get("String_type").map { value =>
        value.asInstanceOf[String] should be("this is a string")
      }
      val future2 = store.get("Int_type").map { value => value.asInstanceOf[Int] should be(1024) }
      val future3 = store.get("Tuple2_type").map { value =>
        value.asInstanceOf[(String, Int)] should be(("element1", 1024))
      }
      val future4 = store.get("key").map { value => value.asInstanceOf[Object] should be(null) }
      Await.result(future1, 15.seconds)
      Await.result(future2, 15.seconds)
      Await.result(future3, 15.seconds)
      Await.result(future4, 15.seconds)
      miniCluster.shutDown
    }
  }
}
