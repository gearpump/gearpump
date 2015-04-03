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

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import org.apache.gearpump.cluster.ClientToMaster.GetJarFileContainer
import org.apache.gearpump.cluster._
import org.apache.gearpump.cluster.master.MasterProxy
import org.apache.gearpump.jarstore.JarFileContainer
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{Constants, LogUtil, Util}
import org.slf4j.Logger

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

trait ClientSubmitter {
  val LOG: Logger = LogUtil.getLogger(getClass)

  implicit val timeout: Timeout
  implicit val system: ActorSystem
  def master: ActorRef

  import system.dispatcher

  /**
   * Submit an applicaiton with default jar setting. Use java property
   * "gearpump.app.jar" if defined. Otherwise, will assume the jar is on
   * the target runtime classpath, and will not send it.
   */
  def submit(app: Application): Int = {
    import Application._
    submitWithJar(app, System.getProperty(GEARPUMP_APP_JAR))
  }

  def submitWithJar(app: AppDescription, jarPath: String): Int = {
    val client = new MasterClient(master)
    val updatedApp = app.copy(name = checkAndAddNamePrefix(app.name, System.getProperty(GEARPUMP_APP_NAME_PREFIX)))
    Option(jarPath) match {
      case Some(path) =>
        client.submitApplication(updatedApp, Option(loadFile(path)))
      case None =>
        client.submitApplication(updatedApp, None)
    }
  }

  def checkAndAddNamePrefix(appName: String, namePrefix: String): String = {
    val fullName = if (namePrefix != null && namePrefix != "") {
      namePrefix + "_" + appName
    } else {
      appName
    }
    if (!Util.validApplicationName(fullName)) {
      close()
      val error = s"The application name $appName is not a proper name. An app name can " +
        "be a sequence of letters, numbers or underscore character \"_\""
      throw new Exception(error)
    }
    fullName
  }

  def loadFile(jarPath: String): AppJar = {
    val jarFile = new java.io.File(jarPath)
    val uploadFile = (master ? GetJarFileContainer).asInstanceOf[Future[JarFileContainer]]
      .map { container =>
      container.copyFromLocal(new java.io.File(jarPath))
      AppJar(jarFile.getName, container)
    }
    Await.result(uploadFile, Duration(15, TimeUnit.SECONDS))
  }

  def close(): Unit = {
    LOG.info(s"Shutting down system ${system.name}")
    system.shutdown()
  }

}

//TODO: add interface to query master here
class ClientContext(config: Config) extends ClientSubmitter {
  private val masters = config.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).toList.flatMap(Util.parseHostList)
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
  implicit val system = ActorSystem(s"client${Util.randInt}" , config)
  LOG.info(s"Starting system ${system.name}")

  val master = system.actorOf(MasterProxy.props(masters), system.name)

  LOG.info(s"Creating master proxy $master for master list: $masters")

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

}

object ClientContext {
  def apply() = new ClientContext(ClusterConfig.load.application)

  def apply(config: Config) = new ClientContext(config)
}
