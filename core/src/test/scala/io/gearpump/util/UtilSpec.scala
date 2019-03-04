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

package io.gearpump.util

import io.gearpump.transport.HostPort
import io.gearpump.util.Util._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.mockito.MockitoSugar

class UtilSpec extends FlatSpec with Matchers with MockitoSugar {
  it should "work" in {

    assert(findFreePort().isSuccess)

    assert(randInt() != randInt())

    val hosts = parseHostList("host1:1,host2:2")
    assert(hosts(1) == HostPort("host2", 2))

    assert(Util.getCurrentClassPath.length > 0)
  }

  it should "check application name properly" in {
    assert(Util.validApplicationName("_application_1"))
    assert(Util.validApplicationName("application_1_"))
    assert(!Util.validApplicationName("0_application_1"))
    assert(!Util.validApplicationName("_applicat&&ion_1"))
  }
}
