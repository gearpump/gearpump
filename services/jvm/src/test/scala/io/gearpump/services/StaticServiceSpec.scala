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

import akka.http.scaladsl.model.headers.`Cache-Control`
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.typesafe.config.Config
import io.gearpump.cluster.TestUtil
import io.gearpump.util.Constants
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import scala.concurrent.duration._

class StaticServiceSpec
  extends FlatSpec with ScalatestRouteTest with Matchers with BeforeAndAfterAll {

  override def testConfig: Config = TestUtil.UI_CONFIG
  private val supervisorPath = system.settings.config.getString(
    Constants.GEARPUMP_SERVICE_SUPERVISOR_PATH)

  protected def route = new StaticService(system, supervisorPath).route

  it should "return version" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/version") ~> route) ~> check {
      val responseBody = responseAs[String]
      assert(responseBody == "Unknown-Version")

      // By default, it will be cached.
      assert(header[`Cache-Control`].isEmpty)
    }
  }

  it should "get correct supervisor path" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/supervisor-actor-path") ~> route) ~> check {
      val responseBody = responseAs[String]
      val defaultSupervisorPath = ""
      assert(responseBody == defaultSupervisorPath)
    }
  }
}
