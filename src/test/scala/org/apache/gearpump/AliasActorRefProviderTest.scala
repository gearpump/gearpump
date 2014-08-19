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

import java.io.{Flushable, Closeable, File}

import akka.actor._
import akka.remote.{AliasActorRefProvider, RemoteScope}
import org.apache.gearpump.util.ActorSystemBooter.{BindLifeCycle, RegisterActorSystem}
import org.apache.gearpump.util.{ActorSystemBooter}
import org.apache.gears.cluster.{DefaultExecutorContext, Configs}
import org.slf4j.{Logger, LoggerFactory}

import scala.sys.process.{ProcessLogger, Process}

object AliasActorRefProviderTest {
  private val LOG: Logger = LoggerFactory.getLogger(AliasActorRefProviderTest.getClass)

  class ProcessLogRedirector extends ProcessLogger with Closeable with Flushable {

    def out(s: => String): Unit = Console.println(s)
    def err(s: => String): Unit = Console.println(s)
    def buffer[T](f: => T): T = f
    def close(): Unit = Unit
    def flush(): Unit = Unit
  }

  class Me extends Actor {
    override def preStart  = {
      val provider = context.system.asInstanceOf[ExtendedActorSystem].provider
      var alias = provider.asInstanceOf[AliasActorRefProvider].getAliasActorRef(self)

      val selfPath = ActorUtil.getFullPath(context)

      Console.println("=============================")
      Console.println(selfPath)

    //  startSystemThere("there", selfPath)
    }

    def receive = {

      case RegisterActorSystem(systemPath) =>
        Console.println(s"Received RegisterActorSystem $systemPath")
        val systemAddress = AddressFromURIString(systemPath)
        val she = context.actorOf( Props(classOf[She], self).withDeploy(Deploy(scope = RemoteScope(systemAddress))), "she")
        sender ! BindLifeCycle(self)

      case (msg : String, ref : ActorRef) =>
        Console.println(s"received $msg..., alias： $ref...," )
        ref ! "Hi, my girl!"
    }
  }

  class She(he : ActorRef) extends Actor {
private  var alias : ActorRef = null

    override def preStart = {
      val provider = context.system.asInstanceOf[ExtendedActorSystem].provider
      if (provider.isInstanceOf[AliasActorRefProvider]) {
        alias = provider.asInstanceOf[AliasActorRefProvider].getAliasActorRef(self)
        Console.println(s"We now have retrived an alias....$alias...for $self")
        he ! ("Hi, boy!", alias)
      }
    }

    def receive = {
      case msg : String =>
        Console.println(s"received msg: $msg")
        he ! ("Hi, boy!", alias)
    }
  }

  def main (args: Array[String]) {

    val adress = AddressFromURIString("s://s2")

    if (args.length == 2) {
      startSystemThere(args(0), args(1))
    } else {
      val here = ActorSystem("here", Configs.SYSTEM_DEFAULT_CONFIG)
      val me = here.actorOf(Props[Me], "me")
      here.awaitTermination()
    }
  }

  def startSystemThere(name : String, reportBack : String) = {
    ActorSystemBooter.create.boot(name, reportBack)
  }
}