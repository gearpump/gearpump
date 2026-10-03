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

import io.gearpump.cluster.AppMasterToMaster.RequestResource

case class AllocateResource(request: RequestResource, applicationCapability: String)
case class InstallLaunchGrant(capability: String, grant: String, appId: Int,
    slots: Int, applicationCapability: String) {
  override def toString: String = "InstallLaunchGrant(<redacted>,appId=" + appId + ",slots=" + slots + ")"
}
case class LaunchGrantInstalled(grant: String) {
  override def toString: String = "LaunchGrantInstalled(<redacted>)"
}
/** Worker-local grant inventory; credentials are checked by the worker before installation. */
class LaunchGrants(now: () => Long = () => System.nanoTime(),
    lifetime: Long = 120000000000L, maxPending: Int = 1024) {
  private case class Grant(appId: Int, slots: Int, application: String, issued: Long)
  private var grants = Map.empty[String, Grant]
  private def expire(): Unit = {
    val current = now()
    grants = grants.filter { case (_, grant) => current - grant.issued < lifetime }
  }
  def install(id: String, appId: Int, slots: Int, application: String): Boolean = synchronized {
    expire()
    if (!ControlCapability.valid(id) || !ControlCapability.valid(application) ||
        appId < 0 || slots <= 0 || grants.size >= maxPending || grants.contains(id)) false
    else { grants += id -> Grant(appId, slots, application, now()); true }
  }
  def consume(id: String, appId: Int, slots: Int, application: String): Boolean = synchronized {
    expire()
    grants.get(id) match {
      case Some(grant) if grant.appId == appId && grant.slots == slots && slots > 0 &&
          ControlCapability.matches(grant.application, application) =>
        grants -= id
        true
      case _ => false
    }
  }
}
