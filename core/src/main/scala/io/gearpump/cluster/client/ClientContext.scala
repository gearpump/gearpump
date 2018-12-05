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

package io.gearpump.cluster.client

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigValueFactory}
import io.gearpump.cluster.ClusterConfig
import io.gearpump.util.{ActorUtil, Constants, LogUtil, Util}
import io.gearpump.cluster.ClientToMaster.{ResolveAppId, ShutdownApplication, SubmitApplication}
import io.gearpump.cluster.MasterToAppMaster.{AppMastersData, AppMastersDataRequest, ReplayFromTimestampWindowTrailingEdge}
import io.gearpump.cluster.MasterToClient._
import io.gearpump.cluster.master.MasterProxy
import io.gearpump.cluster._
import io.gearpump.jarstore.JarStoreClient
import io.gearpump.util.Constants._
import io.gearpump.util.{Constants, LogUtil, Util}
import org.slf4j.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

/**
 * ClientContext is a user facing utility to interact with the master.
 * (e.g. submit/manage an application).
 */
class ClientContext protected(config: Config, sys: ActorSystem, _master: ActorRef) {

  private val LOG: Logger = LogUtil.getLogger(getClass)
  implicit val system = Option(sys).getOrElse(ActorSystem(s"client${Util.randInt()}", config))
  private val jarStoreClient = new JarStoreClient(config, system)
  private val masterClientTimeout = {
    val timeout = Try(config.getInt(Constants.GEARPUMP_MASTERCLIENT_TIMEOUT)).getOrElse(90)
    Timeout(timeout, TimeUnit.SECONDS)
  }

  private lazy val master: ActorRef = {
    val masters = config.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).asScala
      .flatMap(Util.parseHostList)
    val master = Option(_master).getOrElse(system.actorOf(MasterProxy.props(masters),
      s"masterproxy${system.name}"))
    LOG.info(s"Creating master proxy $master for master list: $masters")
    master
  }

  /**
   * Submits an application with default jar setting. Use java property "gearpump.app.jar" if
   * defined. Otherwise, it assumes the jar is on the target runtime classpath, thus will
   * not send the jar across the wire.
   */
  def submit(app: Application): RunningApplication = {
    submit(app, System.getProperty(GEARPUMP_APP_JAR))
  }

  def submit(app: Application, jar: String): RunningApplication = {
    submit(app, jar, getExecutorNum)
  }

  def submit(app: Application, jar: String, executorNum: Option[Int]): RunningApplication = {
    val appName = checkAndAddNamePrefix(app.name, System.getProperty(GEARPUMP_APP_NAME_PREFIX))
    val submissionConfig = getSubmissionConfig(config, executorNum)
    val appDescription =
      AppDescription(appName, app.appMaster.getName, app.userConfig, submissionConfig)
    val appJar = Option(jar).map(loadFile)
    submitApplication(SubmitApplication(appDescription, appJar))
  }

  private def getExecutorNum: Option[Int] = {
    Try(System.getProperty(APPLICATION_EXECUTOR_NUMBER).toInt).toOption
  }

  private def getSubmissionConfig(config: Config, executorNum: Option[Int]): Config = {
    val conf = ClusterConfig.filterOutDefaultConfig(config)
    executorNum match {
      case Some(n) =>
        conf.withValue(APPLICATION_EXECUTOR_NUMBER, ConfigValueFactory.fromAnyRef(n))
      case None =>
        conf
    }
  }

  def listApps: AppMastersData = {
    ActorUtil.askActor[AppMastersData](master, AppMastersDataRequest, masterClientTimeout)
  }

  def replayFromTimestampWindowTrailingEdge(appId: Int): ReplayApplicationResult = {
    val result = Await.result(
      ActorUtil.askAppMaster[ReplayApplicationResult](master,
        appId, ReplayFromTimestampWindowTrailingEdge(appId)), Duration.Inf)
    result
  }

  def askAppMaster[T](appId: Int, msg: Any): Future[T] = {
    ActorUtil.askAppMaster[T](master, appId, msg)
  }

  def shutdown(appId: Int): Unit = {
    val result = ActorUtil.askActor[ShutdownApplicationResult](master,
      ShutdownApplication(appId), masterClientTimeout)
    result.appId match {
      case Success(_) =>
      case Failure(ex) => throw ex
    }
  }

  def resolveAppID(appId: Int): ActorRef = {
    val result = ActorUtil.askActor[ResolveAppIdResult](master,
      ResolveAppId(appId), masterClientTimeout)
    result.appMaster match {
      case Success(appMaster) => appMaster
      case Failure(ex) => throw ex
    }
  }

  def close(): Unit = {
    if (sys == null) {
      LOG.info(s"Shutting down system ${system.name}")
      system.terminate()
    }
  }

  private def loadFile(jarPath: String): AppJar = {
    val jarFile = new java.io.File(jarPath)
    if (!jarFile.exists()) {
      val error = s"File $jarPath does not exist and cannot submit application"
      throw new Exception(error)
    }
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

  private def submitApplication(submitApplication: SubmitApplication): RunningApplication = {
    val result = ActorUtil.askActor[SubmitApplicationResult](master,
      submitApplication, masterClientTimeout)
    val application = result.appId match {
      case Success(appId) =>
        // scalastyle:off println
        Console.println(s"Submit application succeed. The application id is $appId")
        // scalastyle:on println
        new RunningApplication(appId, master, masterClientTimeout)
      case Failure(ex) => throw ex
    }
    application
  }
}

object ClientContext {

  /**
   * Create a [[ClientContext]] which will instantiate an actor system
   * to interact with the master parsed from `gearpump.cluster.masters`.
   * The config is loaded from classpath.
   */
  def apply(): ClientContext = apply(ClusterConfig.default())

  /**
   * Create a [[ClientContext]] which will instantiate an actor system
   * to interact with the master parsed from `gearpump.cluster.masters`
   * through the given config.
   */
  def apply(config: Config): ClientContext = {
    RuntimeEnvironment.newClientContext(config)
  }

  /**
   * Create a [[ClientContext]] for the passed in actor system
   * to interact with the master parsed from `gearpump.cluster.masters`
   * through the given config.
   */
  def apply(config: Config, system: ActorSystem): ClientContext = {
    new ClientContext(config, system, null)
  }

  /**
   * Create a [[ClientContext]] for the passed in actor system
   * to interact with the given master.
   */
  def apply(config: Config, system: ActorSystem, master: ActorRef): ClientContext = {
    new ClientContext(config, system, master)
  }
}