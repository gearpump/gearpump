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

package io.gearpump.cluster.client

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.util.Timeout
import com.typesafe.config.{ConfigFactory, Config}
import io.gearpump.cluster.MasterToAppMaster.{ReplayFromTimestampWindowTrailingEdge, AppMastersData}
import io.gearpump.cluster.MasterToClient.ReplayApplicationResult
import io.gearpump.cluster._
import io.gearpump.cluster.master.MasterProxy
import io.gearpump.jarstore.{FilePath, JarStoreService}
import io.gearpump.util.Constants._
import io.gearpump.util.{ActorUtil, Constants, LogUtil, Util}
import org.slf4j.Logger

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.Future

//TODO: add interface to query master here
class ClientContext(config: Config, sys: ActorSystem, _master: ActorRef) {

  def this(system: ActorSystem) = {
    this(system.settings.config, system, null)
  }

  def this(config: Config) = {
    this(config, null, null)
  }

  private val LOG: Logger = LogUtil.getLogger(getClass)
  private implicit val timeout = Timeout(5, TimeUnit.SECONDS)

  implicit val system = Option(sys).getOrElse(ActorSystem(s"client${Util.randInt}" , config))
  LOG.info(s"Starting system ${system.name}")
  val shouldCleanupSystem = Option(sys).isEmpty

  private val jarStoreService = JarStoreService.get(config)
  jarStoreService.init(config, system)

  private lazy val master: ActorRef = {
    val masters = config.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).toList.flatMap(Util.parseHostList)
    val master = Option(_master).getOrElse(system.actorOf(MasterProxy.props(masters), s"masterproxy${system.name}"))
    LOG.info(s"Creating master proxy ${master} for master list: $masters")
    master
  }

  /**
   * Submit an applicaiton with default jar setting. Use java property
   * "gearpump.app.jar" if defined. Otherwise, will assume the jar is on
   * the target runtime classpath, and will not send it.
   */
  def submit(app : Application) : Int = {
    submit(app, System.getProperty(GEARPUMP_APP_JAR))
  }


  def submit(app : Application, jar: String) : Int = {
    import app.{name, appMaster, userConfig}
    val submissionConfig = getSubmissionConfig(config)
    val appDescription = AppDescription(name, appMaster.getName, userConfig, submissionConfig)
    submit(appDescription, jar)
  }

  import scala.collection.JavaConverters._
  private def getSubmissionConfig(config: Config): Config = {
    val filterReferenceConf = config.entrySet().asScala.foldLeft(ConfigFactory.empty()){(config, entry) =>
      val key = entry.getKey
      val value = entry.getValue
      val origin = value.origin()
      if (origin.resource() == "reference.conf"){
        config
      } else {
        config.withValue(key, value)
      }
    }

    val filterJvmReservedKeys = ClusterConfig.JVM_RESERVED_PROPERTIES.foldLeft(filterReferenceConf){(config, key) =>
      config.withoutPath(key)
    }
    filterJvmReservedKeys
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

  def replayFromTimestampWindowTrailingEdge(appId : Int): ReplayApplicationResult = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val result = Await.result(ActorUtil.askAppMaster[ReplayApplicationResult](master, appId,ReplayFromTimestampWindowTrailingEdge(appId)), Duration.Inf)
    result
  }

  def askAppMaster[T](appId: Int, msg: Any): Future[T] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    ActorUtil.askAppMaster[T](master, appId, msg)
  }

  def listApps: AppMastersData = {
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
    if (shouldCleanupSystem) {
      LOG.info(s"Shutting down system ${system.name}")
      system.shutdown()
    }
  }

  private def loadFile(jarPath : String) : AppJar = {
    val jarFile = new java.io.File(jarPath)
    val path = jarStoreService.copyFromLocal(jarFile)
    AppJar(jarFile.getName, path)
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

  def apply(): ClientContext = new ClientContext(ClusterConfig.load.default, null, null)

  def apply(system: ActorSystem) = new ClientContext(ClusterConfig.load.default, system, null)

  def apply(system: ActorSystem, master: ActorRef) = new ClientContext(ClusterConfig.load.default, system, master)

  def apply(config: Config): ClientContext = new ClientContext(config, null, null)

  def apply(config: Config, system: ActorSystem, master: ActorRef): ClientContext
    = new ClientContext(config, system, master)
}