/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package akka.remote

import akka.actor._
import akka.dispatch.sysmsg._
import akka.event.EventStream
import org.slf4j.{LoggerFactory, Logger}


import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

/**
 * Customized RemoteActorRefProvider to support short remote path
 */

trait AliasActorRefProvider {
  def getAliasActorRef(ref : ActorRef) : ActorRef
}

object AliasRemoteActorRefProvider {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[AliasRemoteActorRefProvider])

  val ALIAS_ACTOR_PATH_SCHEMA = "s://s"

  class AliasActorRefInternal(remote : RemoteTransport, aliasActorPath : AliasActorPath, localAddess: Address) extends  RemoteActorRef(null, null, null, null, null, null) {
    override val localAddressToUse: Address = localAddess
    override val path: ActorPath = aliasActorPath
    override def !(message: Any)(implicit sender: ActorRef = ActorRef.noSender): Unit = {
      remote.send(message, Option(sender), this)
    }
  }

  class AliasActorRef(alias : String, actorRef : InternalActorRef) extends InternalActorRef with ActorRefScope {

    def this(alias : String, actorRef : ActorRef) = this(alias, actorRef.asInstanceOf[InternalActorRef])

    private def remoteTransport(ref : RemoteActorRef) = {
      val remoteField = ref.getClass().getDeclaredField("akka$remote$RemoteActorRef$$remote")
      remoteField.setAccessible(true);
      remoteField.get(ref).asInstanceOf[RemoteTransport]
    }

    @transient private lazy val aliasActorRef = {
      if (actorRef.isInstanceOf[RemoteActorRef]) {
        val ref = actorRef.asInstanceOf[RemoteActorRef]
        new AliasActorRefInternal(remoteTransport(ref), new AliasActorPath(alias, ref.path), ref.localAddressToUse)
      } else {
        null
      }
    }

    def start(): Unit = actorRef.start()
    def resume(causedByFailure: Throwable): Unit  = actorRef.resume(causedByFailure)
    def suspend(): Unit = actorRef.suspend()
    def restart(cause: Throwable): Unit = actorRef.restart(cause)
    def stop(): Unit = actorRef.stop()
    def sendSystemMessage(message: SystemMessage): Unit = actorRef.sendSystemMessage(message)

    def provider: ActorRefProvider = actorRef.provider
    def getParent: InternalActorRef = actorRef.getParent

    def getChild(name: Iterator[String]): InternalActorRef = actorRef.getChild(name)

    def isLocal: Boolean = actorRef.isLocal

    def isTerminated: Boolean = actorRef.isTerminated

    def path: ActorPath = actorRef.path

    def !(message : Any)(implicit sender : ActorRef = ActorRef.noSender) : Unit = {
      if (message == null) throw new InvalidMessageException("Message is null")
      if (null != aliasActorRef) aliasActorRef.!(message)(sender) else actorRef.!(message)(sender)
    }
  }
}

class AliasRemoteActorRefProvider(systemName: String,
                                   settings: ActorSystem.Settings,
                                   eventStream: EventStream,
                                   dynamicAccess: DynamicAccess) extends RemoteActorRefProvider(systemName, settings, eventStream, dynamicAccess) with AliasActorRefProvider {
  import AliasRemoteActorRefProvider._

  val shortPathActorCount = new AtomicInteger(0)
  val shortPathActorMap = new AtomicReference(Map.empty[String, String])

  def getAliasActorRef(ref : ActorRef) : ActorRef = {

    if (ref.isInstanceOf[AliasActorRef]) {
      return ref
    }
    //val alias = ALIAS_ACTOR_PATH_SCHEMA + systemName + "/" + shortPathActorCount.addAndGet(1).toString + "/" + ref.path.name
    val alias = ALIAS_ACTOR_PATH_SCHEMA + shortPathActorCount.addAndGet(1).toString
    val path =  if (ref.path.address.host.isDefined) ref.path.toSerializationFormat else ref.path.toSerializationFormatWithAddress(ref.path.address)

    LOG.debug(s"Register actor short path $path...")

    val pair = alias -> path

    //STM
    var success = false
    while (!success) {
      val oldMap = shortPathActorMap.get()
      val newMap = oldMap + pair
      if (shortPathActorMap.compareAndSet(oldMap, newMap)) {
        success = true
      }
    }
    new AliasActorRef(alias, ref)
  }

  override private[akka] def resolveActorRefWithLocalAddress(path: String, localAddress: Address): InternalActorRef = {
    LOG.debug(s"[$systemName]Resolving path $path...")
    if (path.startsWith(ALIAS_ACTOR_PATH_SCHEMA)) {
      val resolvedPath = shortPathActorMap.get.get(path)

      if (resolvedPath.isEmpty) {
        LOG.debug(s"Canonot resolve the alias path $path in this actor system $systemName...")
        throw new Exception(s"Canonot resolve the alias path $path in this actor system $systemName...")
      }
      LOG.debug(s"After resolving, the real path is $resolvedPath...")
      super.resolveActorRefWithLocalAddress(resolvedPath.get, localAddress)
    } else {
      super.resolveActorRefWithLocalAddress(path, localAddress)
    }
  }
}