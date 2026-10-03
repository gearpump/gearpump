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

package io.gearpump.security

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LaunchGrantsSpec extends AnyFlatSpec with Matchers {
  it should "bind launch authority to the app, resource count and epoch and consume it once" in {
    val grants = new LaunchGrants()
    val token = ControlCapability.random()
    val app = ControlCapability.random()
    assert(grants.install(token, 1, 4, app))
    assert(!grants.consume(token, 2, 4, app))
    assert(!grants.consume(token, 1, 0, app))
    assert(!grants.consume(token, 1, -1, app))
    assert(!grants.consume(token, 1, 5, app))
    assert(!grants.consume(token, 1, 4, ControlCapability.random()))
    assert(grants.consume(token, 1, 4, app))
    assert(!grants.consume(token, 1, 4, app))
  }
  it should "expire and bound pending grants without accepting invalid resources" in {
    var time = 0L
    val grants = new LaunchGrants(() => time, lifetime = 100, maxPending = 1)
    val token = ControlCapability.random()
    val app = ControlCapability.random()
    assert(!grants.install(token, 1, 0, app))
    assert(!grants.install(token, 1, -1, app))
    assert(grants.install(token, 1, 1, app))
    assert(!grants.install(token, 1, 1, app))
    assert(!grants.install(ControlCapability.random(), 1, 1, app))
    time = 100
    assert(!grants.consume(token, 1, 1, app))
    assert(grants.install(ControlCapability.random(), 1, 1, app))
  }
}
