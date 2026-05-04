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

package io.gearpump.transport

import org.apache.pekko.actor._
import io.gearpump.transport.netty.{Context, TaskMessage}
import io.gearpump.transport.netty.Client.Close
import io.gearpump.util.LogUtil
import org.slf4j.Logger
import scala.collection.immutable.LongMap
import scala.concurrent._
import java.util.concurrent.atomic.AtomicReference

trait ActorLookupById {

  /** Lookup actor ref for local task actor by providing a TaskId (TaskId.toLong) */
  def lookupLocalActor(id: Long): Option[ActorRef]
}

/**
 * Custom networking layer.
 *
 * It will translate long sender/receiver address to shorter ones to reduce
 * the network overhead.
 */
class Express(val system: ExtendedActorSystem) extends Extension with ActorLookupById {

  import io.gearpump.transport.Express._
  import system.dispatcher
  private val localActorMap = StateRef(LongMap.empty[ActorRef])
  private val remoteAddressMap = StateRef(Map.empty[Long, HostPort])

  private val remoteClientMap = StateRef(Map.empty[HostPort, ActorRef])

  val conf = system.settings.config

  lazy val (context, serverPort, localHost) = init

  lazy val init = {
    LOG.info(s"Start Express init ...${system.name}")
    val context = new Context(system, conf)
    val serverPort = context.bind("netty-server", this)
    val localHost = HostPort(system.provider.getDefaultAddress.host.get, serverPort)
    LOG.info(s"binding to netty server $localHost")

    system.registerOnTermination(() => context.close())
    (context, serverPort, localHost)
  }

  def unregisterLocalActor(id: Long): Unit = {
    localActorMap.sendOff(_ - id)
  }

  /** Start Netty client actors to connect to remote machines */
  def startClients(hostPorts: Set[HostPort]): Future[Map[HostPort, ActorRef]] = {
    val clientsToClose = remoteClientMap.get().keySet.filterNot(hostPorts.contains)
    closeClients(clientsToClose)
    hostPorts.toList.foldLeft(Future(Map.empty[HostPort, ActorRef])) { (_, hostPort) =>
      remoteClientMap.alter { map =>
        if (!map.contains(hostPort)) {
          val actor = context.connect(hostPort)
          map + (hostPort -> actor)
        } else {
          map
        }
      }
    }
  }

  def closeClients(hostPorts: Set[HostPort]): Future[Map[HostPort, ActorRef]] = {
    remoteClientMap.alter { map =>
      map.foreach {
        case (hostPort, client) if hostPorts.contains(hostPort) =>
          client ! Close
        case _ =>
      }
      map -- hostPorts
    }
  }

  def registerLocalActor(id: Long, actor: ActorRef): Unit = {
    LOG.info(s"RegisterLocalActor: $id, actor: ${actor.path.name}")
    init
    localActorMap.sendOff(_ + (id -> actor))
  }

  def lookupLocalActor(id: Long): Option[ActorRef] = localActorMap.get().get(id)

  def lookupRemoteAddress(id: Long): Option[HostPort] = remoteAddressMap.get().get(id)

  def replaceRemoteAddresses(addresses: Map[Long, HostPort]): Unit = {
    remoteAddressMap.set(addresses)
  }

  def remoteAddressesFuture(): Future[Map[Long, HostPort]] = {
    remoteAddressMap.future()
  }

  /** Send message to remote task */
  def transport(taskMessage: TaskMessage, remote: HostPort): Unit = {

    val remoteClient = remoteClientMap.get().get(remote)
    if (remoteClient.isDefined) {
      remoteClient.get.tell(taskMessage, Actor.noSender)
    } else {
      val errorMsg = s"Clients has not been launched properly before transporting messages, " +
        s"the destination is $remote"
      LOG.error(errorMsg)
      throw new Exception(errorMsg)
    }
  }
}

/** A customized transport layer implemented as a Pekko extension. */
object Express extends ExtensionId[Express] with ExtensionIdProvider {
  val LOG: Logger = LogUtil.getLogger(getClass)

  private final case class StateRef[A](initial: A)(implicit ec: ExecutionContext) {
    private val ref = new AtomicReference[A](initial)

    def get(): A = ref.get()

    def set(value: A): Unit = ref.set(value)

    def future(): Future[A] = Future.successful(get())

    def sendOff(update: A => A): Future[A] = Future(alterNow(update))

    def alter(update: A => A): Future[A] = Future(alterNow(update))

    private def alterNow(update: A => A): A = {
      var updated = false
      var next = ref.get()
      while (!updated) {
        val current = ref.get()
        next = update(current)
        updated = ref.compareAndSet(current, next)
      }
      next
    }
  }

  override def get(system: ActorSystem): Express = super.get(system)

  override def lookup: ExtensionId[Express] = Express

  override def createExtension(system: ExtendedActorSystem): Express = new Express(system)
}
