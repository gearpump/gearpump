/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.gearpump.cluster.client

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

import akka.actor.{ActorRef, ActorSystem}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigValueFactory}
import org.slf4j.Logger

import org.apache.gearpump.cluster.MasterToAppMaster.{AppMastersData, ReplayFromTimestampWindowTrailingEdge}
import org.apache.gearpump.cluster.MasterToClient.ReplayApplicationResult
import org.apache.gearpump.cluster._
import org.apache.gearpump.cluster.master.MasterProxy
import org.apache.gearpump.jarstore.{JarStoreClient, JarStoreServer}
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{ActorUtil, Constants, LogUtil, Util}

/**
 * ClientContext is a user facing util to submit/manage an application.
 *
 * TODO: add interface to query master here
 */
class ClientContext(config: Config, sys: ActorSystem, _master: ActorRef) {

  def this(system: ActorSystem) = {
    this(system.settings.config, system, null)
  }

  def this(config: Config) = {
    this(config, null, null)
  }

  private val LOG: Logger = LogUtil.getLogger(getClass)
  private implicit val timeout = Timeout(5, TimeUnit.SECONDS)

  implicit val system = Option(sys).getOrElse(ActorSystem(s"client${Util.randInt()}", config))
  LOG.info(s"Starting system ${system.name}")
  val shouldCleanupSystem = Option(sys).isEmpty

  private val jarStoreClient = new JarStoreClient(config, system)

  private lazy val master: ActorRef = {
    val masters = config.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).asScala
      .flatMap(Util.parseHostList)
    val master = Option(_master).getOrElse(system.actorOf(MasterProxy.props(masters),
      s"masterproxy${system.name}"))
    LOG.info(s"Creating master proxy ${master} for master list: $masters")
    master
  }

  /**
   * Submits an application with default jar setting. Use java property "gearpump.app.jar" if
   * defined. Otherwise, it assumes the jar is on the target runtime classpath, thus will
   * not send the jar across the wire.
   */
  def submit(app: Application): Int = {
    submit(app, System.getProperty(GEARPUMP_APP_JAR))
  }

  def submit(app: Application, jar: String): Int = {
    submit(app, jar, getExecutorNum())
  }

  def submit(app: Application, jar: String, executorNum: Int): Int = {
    val client = getMasterClient
    val appName = checkAndAddNamePrefix(app.name, System.getProperty(GEARPUMP_APP_NAME_PREFIX))
    val submissionConfig = getSubmissionConfig(config)
      .withValue(APPLICATION_EXECUTOR_NUMBER, ConfigValueFactory.fromAnyRef(executorNum))
    val appDescription =
      AppDescription(appName, app.appMaster.getName, app.userConfig, submissionConfig)
    val appJar = Option(jar).map(loadFile)
    client.submitApplication(appDescription, appJar)
  }

  private def getExecutorNum(): Int = {
    Try(System.getProperty(APPLICATION_EXECUTOR_NUMBER).toInt).getOrElse(1)
  }

  private def getSubmissionConfig(config: Config): Config = {
    ClusterConfig.filterOutDefaultConfig(config)
  }

  def replayFromTimestampWindowTrailingEdge(appId: Int): ReplayApplicationResult = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val result = Await.result(
      ActorUtil.askAppMaster[ReplayApplicationResult](master,
        appId, ReplayFromTimestampWindowTrailingEdge(appId)), Duration.Inf)
    result
  }

  def askAppMaster[T](appId: Int, msg: Any): Future[T] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    ActorUtil.askAppMaster[T](master, appId, msg)
  }

  def listApps: AppMastersData = {
    val client = getMasterClient
    client.listApplications
  }

  def shutdown(appId: Int): Unit = {
    val client = getMasterClient
    client.shutdownApplication(appId)
  }

  def resolveAppID(appId: Int): ActorRef = {
    val client = getMasterClient
    client.resolveAppId(appId)
  }

  def close(): Unit = {
    if (shouldCleanupSystem) {
      LOG.info(s"Shutting down system ${system.name}")
      system.terminate()
    }
  }

  private def loadFile(jarPath: String): AppJar = {
    val jarFile = new java.io.File(jarPath)
    Util.uploadJar(jarFile, jarStoreClient)
  }

  private def checkAndAddNamePrefix(appName: String, namePrefix: String): String = {
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

  private def getMasterClient: MasterClient = {
    val timeout = Try(config.getInt(Constants.GEARPUMP_MASTERCLIENT_TIMEOUT)).getOrElse(90)
    new MasterClient(master, akka.util.Timeout(timeout, TimeUnit.SECONDS))
  }
}

object ClientContext {

  def apply(): ClientContext = new ClientContext(ClusterConfig.default(), null, null)

  def apply(system: ActorSystem): ClientContext = {
    new ClientContext(ClusterConfig.default(), system, null)
  }

  def apply(system: ActorSystem, master: ActorRef): ClientContext = {
    new ClientContext(ClusterConfig.default(), system, master)
  }

  def apply(config: Config): ClientContext = new ClientContext(config, null, null)

  def apply(config: Config, system: ActorSystem, master: ActorRef): ClientContext = {
    new ClientContext(config, system, master)
  }
}