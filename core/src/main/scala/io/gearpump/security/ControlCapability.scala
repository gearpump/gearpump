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

import com.typesafe.config.{Config, ConfigValueFactory}
import io.gearpump.cluster.AppMasterToMaster._
import io.gearpump.cluster.ClientToMaster._
import io.gearpump.cluster.WorkerToMaster._
import java.security.{MessageDigest, SecureRandom}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

/** A capability is authority, not a caller-supplied actor address or application ID. */
case class ControlRequest(capability: String, appId: Option[Int], message: Any) {
  override def toString: String = "ControlRequest(<redacted>," + appId + "," +
    message.getClass.getSimpleName + ")"
}
case class KvReply(capability: String, message: Any) {
  override def toString: String = "KvReply(<redacted>," + message.getClass.getSimpleName + ")"
}
case class GetApplicationControl(appId: Int)
case class ApplicationControl(appId: Int, capability: String) {
  override def toString: String = "ApplicationControl(" + appId + ",<redacted>)"
}
object ControlCapability {
  val AdminKey = "gearpump.security.control-secret"
  val AppKey = "gearpump.security.application-capability"
  val AppId = "gearpump.security.application-id"
  def random(): String = {
    val bytes = new Array[Byte](32)
    new SecureRandom().nextBytes(bytes)
    Base64.getUrlEncoder.withoutPadding().encodeToString(bytes)
  }
  def valid(value: String): Boolean = value != null && value.matches("[A-Za-z0-9_-]{43,128}")
  def matches(expected: String, actual: String): Boolean = valid(expected) && valid(actual) &&
    MessageDigest.isEqual(expected.getBytes(UTF_8), actual.getBytes(UTF_8))
  def token(config: Config, key: String): String =
    if (config.hasPath(key)) config.getString(key) else ""
  def wrap(config: Config, message: Any): ControlRequest = {
    val app = token(config, AppKey)
    if (valid(app)) ControlRequest(app, Some(config.getInt(AppId)), message)
    else ControlRequest(token(config, AdminKey), None, message)
  }
  def protectedMessage(message: Any): Boolean = message match {
    case _: SubmitApplication | _: RestartApplication | _: ShutdownApplication |
         _: RegisterAppMaster | _: ApplicationStatusChanged | _: SaveAppData | _: GetAppData |
         _: RequestResource | _: QueryAppMasterConfig | QueryMasterConfig | GetJarStoreServer |
         _: io.gearpump.cluster.scheduler.Scheduler.ApplicationFinished |
         _: GetApplicationControl | RegisterNewWorker | _: RegisterWorker | _: ResourceUpdate => true
    case _ => false
  }
  def applicationId(message: Any): Option[Int] = message match {
    case m: RegisterAppMaster => Some(m.appId)
    case m: ApplicationStatusChanged => Some(m.appId)
    case m: SaveAppData => Some(m.appId)
    case m: GetAppData => Some(m.appId)
    case m: RequestResource => Some(m.appId)
    case m: ShutdownApplication => Some(m.appId)
    case _ => None
  }
  def applicationRequest(message: Any): Boolean = applicationId(message).isDefined ||
    message == GetJarStoreServer
  def ensureTransport(config: Config): Unit = {
    import scala.jdk.CollectionConverters._
    if (config.getString("pekko.actor.provider") != "local") {
      require(config.getStringList("pekko.remote.classic.enabled-transports").asScala.toList ==
        List("pekko.remote.classic.netty.ssl") &&
        config.getBoolean("pekko.remote.classic.netty.ssl.enable-ssl") &&
        config.getString("pekko.remote.classic.netty.ssl.ssl-engine-provider") ==
          "io.gearpump.security.ClusterSSLEngineProvider", "Cluster control requires mutual TLS")
    }
  }
  def runtimeConfig(config: Config, appId: Int, capability: String): Config = {
    config.withValue(AdminKey, ConfigValueFactory.fromAnyRef(""))
      .withValue("gearpump.security.worker-secret", ConfigValueFactory.fromAnyRef(""))
      .withValue(AppKey, ConfigValueFactory.fromAnyRef(capability))
      .withValue(AppId, ConfigValueFactory.fromAnyRef(appId))
  }
  def redact(config: Config): Config = config.withoutPath("gearpump.security")
    .withoutPath("gearpump.ui-security").withoutPath("pekko.http.session")
    .withoutPath("akka.http.session")
    .withoutPath("gearpump.jarstore.access-token").withoutPath("pekko.remote.classic.netty.ssl.security")
}
