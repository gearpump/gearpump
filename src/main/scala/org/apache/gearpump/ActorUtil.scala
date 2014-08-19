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

package org.apache.gearpump

import akka.actor.Actor.Receive
import akka.actor._
import akka.remote.{AliasActorRefProvider, AliasRemoteActorRefProvider}
import org.slf4j.{Logger, LoggerFactory}

object ActorUtil {
   private val LOG: Logger = LoggerFactory.getLogger(ActorUtil.getClass)

  def getSystemPath(system : ActorSystem) : String = {
    system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress.toString
  }

   def getFullPath(context: ActorContext): String = {
     context.self.path.toStringWithAddress(
       context.system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress)
   }

   def defaultMsgHandler(actor : ActorRef) : Receive = {
     case msg : Any =>
       LOG.error(s"Cannot find a matching message, ${msg.getClass.toString}, forwarded from $actor")
   }

  def getAliasActorRef(context : ActorContext, ref : ActorRef) = {
    val provider = context.system.asInstanceOf[ExtendedActorSystem].provider

    if(provider.isInstanceOf[AliasActorRefProvider]) {
      provider.asInstanceOf[AliasActorRefProvider].getAliasActorRef(ref)
    } else {
      ref
    }
  }

  def printActorSystemTree(system : ActorSystem) : Unit = {
    val extendedSystem = system.asInstanceOf[ExtendedActorSystem]
    val clazz = system.getClass
    val m = clazz.getDeclaredMethod("printTree")
    m.setAccessible(true);//Abracadabra
    Console.out.print(m.invoke(system))
  }
 }
