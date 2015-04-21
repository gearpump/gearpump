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
import akka.actor.{ActorRef, ActorSystem}
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

//TODO: add interface to query master here
class ClientContext(config: Config) {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  private implicit val timeout = Timeout(5, TimeUnit.SECONDS)

  private val masters = config.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).toList.flatMap(Util.parseHostList)

  implicit val system = ActorSystem(s"client${Util.randInt}" , config)
  LOG.info(s"Starting system ${system.name}")

  import system.dispatcher

  private val master = system.actorOf(MasterProxy.props(masters), system.name)

  LOG.info(s"Creating master proxy ${master} for master list: $masters")

  /**
   * Submit an applicaiton with default jar setting. Use java property
   * "gearpump.app.jar" if defined. Otherwise, will assume the jar is on
   * the target runtime classpath, and will not send it.
   */
  def submit(app : Application) : Int = {
    import app.{name, appMaster, userConfig}
    val clusterConfig = ClusterConfig.load.applicationSubmissionConfig
    val appDescription = AppDescription(name, appMaster.getName, userConfig, clusterConfig)
    submit(appDescription, System.getProperty(GEARPUMP_APP_JAR))
  }

  private def submit(app : AppDescription, jarPath: String) : Int = {
    val client = new MasterClient(master)
    val appName = checkAndAddNamePrefix(app.name, System.getProperty(GEARPUMP_APP_NAME_PREFIX))
    val updatedApp = AppDescription(appName, app.appMaster, app.userConfig, app.clusterConfig)
    if (jarPath == null) {
      client.submitApplication(updatedApp, None)
    } else {
      val appJar = loadFile(jarPath)
      client.submitApplication(updatedApp, Option(appJar))
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
    LOG.info(s"Shutting down system ${system.name}")
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

  private def checkAndAddNamePrefix(appName: String, namePrefix: String) : String = {
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
}

object ClientContext {
  def apply() = new ClientContext(ClusterConfig.load.default)

  def apply(config: Config) = new ClientContext(config)
}
