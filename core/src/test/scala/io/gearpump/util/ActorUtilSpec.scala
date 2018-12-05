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
import org.scalatest.FlatSpec

class ActorUtilSpec extends FlatSpec {
  "masterActorPath" should "construct the ActorPath from HostPort" in {
    import Constants.MASTER

    val host = "127.0.0.1"
    val port = 3000
    val master = HostPort("127.0.0.1", 3000)
    val masterPath = ActorUtil.getMasterActorPath(master)
    assert(masterPath.address.port == Some(port))
    assert(masterPath.address.system == MASTER)
    assert(masterPath.address.host == Some(host))
    assert(masterPath.address.protocol == "akka.tcp")
    assert(masterPath.toStringWithoutAddress == s"/user/$MASTER")
  }
}

