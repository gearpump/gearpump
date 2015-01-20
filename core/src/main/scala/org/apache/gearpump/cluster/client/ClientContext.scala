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

package org.apache.gearpump.cluster.client

import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout
import org.apache.gearpump.cluster.ClientToMaster.GetJarFileContainer
import org.apache.gearpump.cluster._
import org.apache.gearpump.cluster.master.MasterProxy
import org.apache.gearpump.jarstore.JarFileContainer
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{LogUtil, Util}
import org.slf4j.Logger

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContextExecutor, Await, Future}

//TODO: add interface to query master here
class ClientContext(masters: Iterable[HostPort]) {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  private implicit val timeout = Timeout(5, TimeUnit.SECONDS)

  private val config = ClusterConfig.load.application

  implicit val system = ActorSystem(s"client${Util.randInt}" , config)
  import system.dispatcher

  private val master = system.actorOf(Props(classOf[MasterProxy], masters), MASTER)

  LOG.info(s"Creating master proxy ${master} for master list: $masters")

  /**
   * Submit an applicaiton with default jar setting. Use java property
   * "gearpump.app.jar" if defined. Otherwise, will assume the jar is on
   * the target runtime classpath, and will not send it.
   */
  def submit(app : Application) : Int = {
    submit(app, System.getProperty(GEARPUMP_APP_JAR))
  }

  def submit(app : Application, jarPath: String) : Int = {
    val client = new MasterClient(master)
    if (jarPath == null) {
      client.submitApplication(app, None)
    } else {
      val appJar = loadFile(jarPath)
      client.submitApplication(app, Option(appJar))
    }
  }

  def replayFromTimestampWindowTrailingEdge(appId : Int) = {
    val client = new MasterClient(master)
    client.replayFromTimestampWindowTrailingEdge(appId)
  }

  def listApps = {
    val client = new MasterClient(master)
    client.listApplications
  }

  def shutdown(appId : Int) : Unit = {
    val client = new MasterClient(master)
    client.shutdownApplication(appId)
  }

  def resolveAppID(appId: Int) : ActorRef = {
    val client = new MasterClient(master)
    client.resolveAppId(appId)
  }

  def close() : Unit = {
    system.shutdown()
  }

  private def loadFile(jarPath : String) : AppJar = {
    val jarFile = new java.io.File(jarPath)

    val uploadFile = (master ? GetJarFileContainer).asInstanceOf[Future[JarFileContainer]]
      .map {container =>
      container.copyFromLocal(new java.io.File(jarPath))
      AppJar(jarFile.getName, container)
    }

    Await.result(uploadFile, Duration(15, TimeUnit.SECONDS))
  }
}

object ClientContext {
  /**
   * masterList is a list of master node address
   * host1:port,host2:port2,host3:port3
   */
  def apply(masterList : String) = {
    new ClientContext(Util.parseHostList(masterList))
  }

  def apply(masters: Iterable[HostPort]) = new ClientContext(masters)
}
