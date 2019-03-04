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

package io.gearpump.services

import akka.actor.ActorSystem
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.typesafe.config.Config
import io.gearpump.cluster.TestUtil
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import scala.concurrent.Await
import scala.concurrent.duration._

class AdminServiceSpec
  extends FlatSpec with ScalatestRouteTest with Matchers with BeforeAndAfterAll {

  override def testConfig: Config = TestUtil.DEFAULT_CONFIG

  implicit def actorSystem: ActorSystem = system

  it should "shutdown the ActorSystem when receiving terminate" in {
    val route = new AdminService(actorSystem).route
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Post(s"/terminate") ~> route) ~> check {
      assert(status.intValue() == 404)
    }

    Await.result(actorSystem.whenTerminated, 20.seconds)

    // terminate should terminate current actor system
    assert(actorSystem.whenTerminated.isCompleted)
  }
}
