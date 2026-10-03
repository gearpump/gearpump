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

package io.gearpump.cluster.master

import org.apache.pekko.actor._
import org.apache.pekko.cluster.ddata.{DistributedData, LWWMap, LWWMapKey}
import org.apache.pekko.cluster.ddata.Replicator._
import io.gearpump.util.LogUtil
import java.util.concurrent.TimeUnit
import org.slf4j.Logger
import scala.concurrent.TimeoutException
import scala.concurrent.duration.Duration

/**
 * A replicated simple in-memory KV service. The replications are stored on all masters.
 */
class InMemoryKVService extends Actor with Stash {
  import InMemoryKVService._

  private val KV_SERVICE = "gearpump_kvservice"

  private val LOG: Logger = LogUtil.getLogger(getClass)
  private val distributedData = DistributedData(context.system)
  private val replicator = distributedData.replicator

  // Optimize write path, we can tolerate one master down for recovery.
  private val timeout = Duration(15, TimeUnit.SECONDS)
  private val readMajority = ReadMajority(timeout)
  private val writeMajority = WriteMajority(timeout)

  private def groupKey(group: String): LWWMapKey[Any, Any] = {
    LWWMapKey[Any, Any](KV_SERVICE + "_" + group)
  }

  private val secret = io.gearpump.security.ControlCapability.token(context.system.settings.config,
    io.gearpump.security.ControlCapability.AdminKey)
  require(io.gearpump.security.ControlCapability.valid(secret), "Metadata requires control-secret")
  private def reply(client: ActorRef, result: Any): Unit =
    client ! io.gearpump.security.KvReply(secret, result)
  def receive: Receive = {
    case request: io.gearpump.security.ControlRequest if request.appId.isEmpty &&
        io.gearpump.security.ControlCapability.matches(secret, request.capability) =>
      request.message match {
        case _: GetKV | _: PutKV | _: DeleteKVGroup => kvService.apply(request.message)
        case _ => sender() ! Status.Failure(new SecurityException("Metadata operation denied"))
      }
    case _: GetKV | _: PutKV | _: DeleteKVGroup | _: io.gearpump.security.ControlRequest =>
      sender() ! Status.Failure(new SecurityException("Metadata capability required"))
    case message if sender() == replicator => kvService.applyOrElse(message, unhandled _)
    case _ => sender() ! Status.Failure(new SecurityException("Metadata operation denied"))
  }

  def kvService: Receive = {

    case GetKV(group: String, key: String) =>
      val request = Request(sender(), key)
      replicator ! Get(groupKey(group), readMajority, Some(request))
    case success@GetSuccess(group: LWWMapKey[Any @unchecked, Any @unchecked],
    Some(request: Request)) =>
      val appData = success.get(group)
      LOG.info(s"Successfully retrived group: ${group.id}")
      reply(request.client, GetKVSuccess(request.key, appData.get(request.key).orNull))
    case NotFound(group: LWWMapKey[Any @unchecked, Any @unchecked], Some(request: Request)) =>
      LOG.info(s"We cannot find group $group")
      reply(request.client, GetKVSuccess(request.key, null))
    case GetFailure(_, Some(request: Request)) =>
      val error = s"Failed to get application data, the request key is ${request.key}"
      LOG.error(error)
      reply(request.client, GetKVFailed(new Exception(error)))

    case PutKV(group: String, key: String, value: Any) =>
      val request = Request(sender(), key)
      val update = Update(groupKey(group), LWWMap(), writeMajority, Some(request)) { map =>
        map.put(distributedData.selfUniqueAddress, key, value)
      }
      replicator ! update
    case UpdateSuccess(_, Some(request: Request)) =>
      reply(request.client, PutKVSuccess)
    case ModifyFailure(_, error, cause,
    Some(request: Request)) =>
      reply(request.client, PutKVFailed(request.key, new Exception(error, cause)))
    case UpdateTimeout(_, Some(request: Request)) =>
      reply(request.client, PutKVFailed(request.key, new TimeoutException()))

    case DeleteKVGroup(group: String) =>
      replicator ! Delete(groupKey(group), writeMajority)
    case DeleteSuccess(group, _) =>
      LOG.info(s"KV Group ${group.id} is deleted")
    case ReplicationDeleteFailure(group, _) =>
      LOG.error(s"Failed to delete KV Group ${group.id}...")
    case DataDeleted(group, _) =>
      LOG.error(s"Group ${group.id} is deleted, you can no longer put/get/delete this group...")
  }
}

object InMemoryKVService {
  /**
   * KV Service related
   */
  case class GetKV(group: String, key: String)

  trait GetKVResult

  case class GetKVSuccess(key: String, value: Any) extends GetKVResult

  case class GetKVFailed(ex: Throwable) extends GetKVResult

  case class PutKV(group: String, key: String, value: Any)

  case class DeleteKVGroup(group: String)

  case class GroupDeleted(group: String) extends GetKVResult with PutKVResult

  trait PutKVResult

  case object PutKVSuccess extends PutKVResult

  case class PutKVFailed(key: String, ex: Throwable) extends PutKVResult

  case class Request(client: ActorRef, key: String)
}
